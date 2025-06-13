# Copyright 2024 SkyPilot Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Google Cloud Platform (GCP) backend for serverless functions."""

from typing import Dict, List, Any, Optional, Callable
import os
import colorama
import shutil
import subprocess
import tempfile
import re
import datetime
import threading
import hashlib
import pathlib
import sys

# pylint: disable=import-error
from google.api_core import exceptions as google_exceptions
from google.api_core import operation
from google.cloud.devtools import cloudbuild_v1 as build_v1
from google.cloud import run_v2
from google.cloud import storage
from google.cloud import functions_v2
from google.cloud import iam_admin_v1
from google.cloud import scheduler_v1
from google.cloud import pubsub_v1
from google.cloud import artifactregistry_v1
from google.api_core.exceptions import NotFound
from google.protobuf import field_mask_pb2

import sky
from sky.clouds import gcp as gcp_cloud
from sky.serverless import data_models
from sky.serverless import exceptions as serverless_exceptions
from sky.serverless.backends import abstract_backend
from sky.utils import ux_utils
from sky.utils.rich_console_utils import get_console

_SKYPILOT_WHEEL_PATH: Optional[str] = None
_SKYPILOT_WHEEL_BUILD_LOCK = threading.Lock()

# TODO: Use a more restricted set of permissions.
GUARDIAN_ROLE_PERMISSIONS = [
    "compute.instances.create",
    "compute.instances.get",
    "compute.instances.list",
    "compute.instances.setLabels",
    "compute.instances.setMetadata",
    "compute.instances.setServiceAccount",
    "compute.instances.start",
    "compute.instances.stop",
    "compute.zoneOperations.get",
    "iam.serviceAccounts.actAs",
    "iam.roles.get",
    "resourcemanager.projects.getIamPolicy",
]


class GCPBackend(abstract_backend.AbstractServerlessBackend):
    """GCP backend implementation for serverless functions."""

    def __init__(self):
        super().__init__()
        self.project_id = gcp_cloud.GCP.get_project_id()
        self.functions_client = functions_v2.FunctionServiceClient()
        self.storage_client = storage.Client(project=self.project_id)
        self.iam_client = iam_admin_v1.IAMClient()
        self.scheduler_client = scheduler_v1.CloudSchedulerClient()
        self.publisher_client = pubsub_v1.PublisherClient()
        self.subscriber_client = pubsub_v1.SubscriberClient()

    def _compute_source_hash(self, source_dir: str) -> str:
        """Computes a SHA256 hash of the source directory's contents."""
        hasher = hashlib.sha256()
        for root, _, files in sorted(os.walk(source_dir)):
            for filename in sorted(files):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, "rb") as f:
                        while chunk := f.read(8192):
                            hasher.update(chunk)
                except OSError:
                    continue
        return hasher.hexdigest()

    def _setup_iam_for_guardian(self, region: str) -> str:
        """
        Uses the current active gcloud user/service account for the Guardian.

        This is a workaround that bypasses the creation of a dedicated service
        account and role, directly using the user's active credentials.

        Returns:
            The email of the active gcloud account.
        """
        # Bypassing the creation of a dedicated role and service account.
        # Instead, we will use the user's currently active account.
        # This requires the active account to have the necessary permissions.
        print(
            f"{colorama.Fore.YELLOW}INFO: Bypassing dedicated service "
            "account creation. Using the current active gcloud "
            f"user/service account.{colorama.Style.RESET_ALL}"
        )

        try:
            # gcp_cloud is from 'from sky.clouds import gcp as gcp_cloud'
            # This method gets the email of the active gcloud user.
            user_identity = gcp_cloud.GCP.get_active_user_identity()
            if not user_identity:
                raise sky.exceptions.CloudUserIdentityError(
                    "Could not get active GCP user identity."
                )

            full_identity_string = user_identity[0]
            print(f"Using active identity: {full_identity_string}")

            # The identity string is "email [project_id=...]".
            # We must extract only the email part for the API call.
            active_account_email = full_identity_string.split(" ")[0]

            return active_account_email

        except sky.exceptions.CloudUserIdentityError as e:
            with ux_utils.print_exception_no_traceback():
                raise RuntimeError(
                    "Failed to get active GCP user identity. Please make sure "
                    "you are logged in with `gcloud auth login` and "
                    "`gcloud auth application-default login`."
                ) from e

    def _check_apis_enabled(self):
        """Checks if required APIs for serverless are enabled."""
        # This part is better done with gcloud as it's a one-time setup check.
        # But to adhere to the pure SDK principle, we'll keep it as is
        # and just add Cloud Build API to the check list.
        console = get_console()
        with console.status(ux_utils.spinner_message("Checking required GCP APIs...")):
            required_apis = [
                ("run.googleapis.com", "Cloud Run API"),
                ("cloudbuild.googleapis.com", "Cloud Build API"),
                ("iam.googleapis.com", "Identity and Access Management (IAM) API"),
                ("pubsub.googleapis.com", "Cloud Pub/Sub API"),
                ("cloudscheduler.googleapis.com", "Cloud Scheduler API"),
                ("eventarc.googleapis.com", "Eventarc API"),
            ]

            # This check is more robustly done via gcloud services list.
            # A pure SDK check is more complex, involving service usage APIs.
            # For simplicity and robustness, we will keep the gcloud check here
            # as it is part of the environment setup, not core deployment logic.
            # If a pure SDK check is strictly required, it would involve
            # google.api.servicemanagement_v1 and serviceusage_v1 clients.
            disabled_apis = []
            for api_endpoint, api_name in required_apis:
                check_cmd = (
                    f"gcloud services list --project {self.project_id} "
                    f'--filter="config.name={api_endpoint}" --format="value(state)"'
                )
                proc = subprocess.run(
                    check_cmd, shell=True, capture_output=True, text=True
                )
                if "ENABLED" not in proc.stdout:
                    disabled_apis.append((api_endpoint, api_name))

        if disabled_apis:
            error_msg = (
                f"{colorama.Fore.RED}Error: The following required APIs are disabled for project "
                f"{self.project_id}.{colorama.Style.RESET_ALL}\n"
            )
            for api_endpoint, api_name in disabled_apis:
                error_msg += f"  - {api_name} ({api_endpoint})\n"
            error_msg += "\nPlease enable them by running the following command(s):\n"
            for api_endpoint, _ in disabled_apis:
                error_msg += f"  {colorama.Style.BRIGHT}gcloud services enable {api_endpoint} --project={self.project_id}{colorama.Style.RESET_ALL}\n"
            error_msg += "\nAfter enabling, please wait a few minutes and retry."
            raise serverless_exceptions.CloudAPIDisabledError(error_msg)

        console.print(ux_utils.finishing_message("All required GCP APIs are enabled."))

    def _build_skypilot_wheel_if_needed(self) -> str:
        """Builds a SkyPilot wheel and caches the path."""
        global _SKYPILOT_WHEEL_PATH
        with _SKYPILOT_WHEEL_BUILD_LOCK:
            if _SKYPILOT_WHEEL_PATH is not None and os.path.exists(
                _SKYPILOT_WHEEL_PATH
            ):
                return _SKYPILOT_WHEEL_PATH

            sky_source_path = os.path.dirname(os.path.dirname(sky.__file__))
            dist_dir = os.path.join(sky_source_path, "dist")

            if os.path.exists(dist_dir):
                shutil.rmtree(dist_dir)

            try:
                command = [
                    sys.executable,
                    "-m",
                    "build",
                    "--wheel",
                    "--outdir",
                    dist_dir,
                ]
                subprocess.run(
                    command,
                    cwd=sky_source_path,
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                error_message = (
                    "Failed to build SkyPilot wheel package.\n"
                    f"Command: {' '.join(command)}\n"
                    f"Exit Code: {e.returncode}\n"
                    f"Stderr:\n{e.stderr}"
                )
                if "No module named 'build'" in e.stderr:
                    error_message += "\n\nHint: The build tool seems to be missing. Please run 'pip install build' in your environment and try again."
                raise RuntimeError(error_message) from e

            wheel_files = list(pathlib.Path(dist_dir).glob("*.whl"))
            if not wheel_files:
                raise RuntimeError(
                    "Failed to build SkyPilot wheel, but "
                    "the build command succeeded. Check dist/ dir."
                )

            _SKYPILOT_WHEEL_PATH = str(wheel_files[0])
            return _SKYPILOT_WHEEL_PATH

    def _prepare_source_for_deployment(
        self, func: data_models.ServerlessFunction
    ) -> str:
        """
        Prepares a clean, standard deployment package.
        """
        skypilot_wheel_path = self._build_skypilot_wheel_if_needed()
        skypilot_wheel_name = os.path.basename(skypilot_wheel_path)
        temp_dir = tempfile.mkdtemp(prefix=f"{func.name}-source-")
        shutil.copytree(func.code_path, temp_dir, dirs_exist_ok=True)
        shutil.copy(skypilot_wheel_path, os.path.join(temp_dir, skypilot_wheel_name))

        final_req_path = os.path.join(temp_dir, "requirements.txt")
        with open(final_req_path, "w") as final_f:
            final_f.write("# SkyPilot Core Package\n")
            final_f.write(f"./{skypilot_wheel_name}\n\n")
            
            final_f.write('# Web Server Dependencies\n')
            final_f.write('gunicorn\n')
            final_f.write('uvicorn\n')
            final_f.write('\n')

            base_req_path = os.path.join(func.code_path, "requirements.txt")
            if os.path.exists(base_req_path):
                final_f.write("# Generic Dependencies from requirements.txt\n")
                with open(base_req_path, "r") as base_f:
                    final_f.write(base_f.read())
                final_f.write("\n\n")

            if func.extra_requirements:
                final_f.write("# Platform-Specific or Extra Dependencies\n")
                for req_file in func.extra_requirements:
                    extra_req_path = os.path.join(func.code_path, req_file)
                    if os.path.exists(extra_req_path):
                        with open(extra_req_path, "r") as extra_f:
                            final_f.write(f"# From: {req_file}\n")
                            final_f.write(extra_f.read())
                        final_f.write("\n")

        module, app_object = func.entrypoint.split(":")
        procfile_path = os.path.join(temp_dir, "Procfile")
        with open(procfile_path, "w") as f:
            f.write(
                f"web: gunicorn -w 1 -k uvicorn.workers.UvicornWorker -b :$PORT {module}:{app_object}\n"
            )

        if func.runtime.startswith("python"):
            match = re.match(r"python(\d)(\d+)", func.runtime)
            if match:
                major, minor = match.groups()
                python_version_str = f"{major}.{minor}"
                python_version_file = os.path.join(temp_dir, ".python-version")
                with open(python_version_file, "w") as f:
                    f.write(python_version_str)

        return temp_dir

    def _upload_source_to_gcs(self, source_dir: str, region: str) -> str:
        """
        Zips the source directory and uploads it to a GCS bucket.

        Args:
            source_dir: The path to the directory to upload.
            region: The GCP region for the GCS bucket.

        Returns:
            The GCS URI of the uploaded source archive (e.g., gs://bucket/object).
        """
        storage_client = storage.Client(project=self.project_id)
        bucket_name = f"cloud-run-source-deploy-{self.project_id}-{region}"
        bucket = storage_client.bucket(bucket_name)

        if not bucket.exists():
            bucket.create(location=region)

        # Zip the source directory
        archive_path = shutil.make_archive(
            base_name=os.path.join(source_dir, "source"),
            format="zip",
            root_dir=source_dir,
        )
        blob_name = f"source-{os.path.basename(archive_path)}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(archive_path)

        os.remove(archive_path)
        return f"gs://{bucket_name}/{blob_name}"

    def _ensure_artifact_registry_repo_exists(
        self,
        region: str,
        repo_id: str,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        """Checks if an Artifact Registry repository exists, creates it if not."""

        def _update_status(message: str):
            if status_callback:
                status_callback(message)

        _update_status(f"Checking for Artifact Registry repository '{repo_id}'...")
        ar_client = artifactregistry_v1.ArtifactRegistryClient()
        repo_parent = f"projects/{self.project_id}/locations/{region}"
        repo_name_full = f"{repo_parent}/repositories/{repo_id}"

        try:
            ar_client.get_repository(name=repo_name_full)
        except NotFound:
            _update_status(f"Repository '{repo_id}' not found. Creating it now...")
            repository = artifactregistry_v1.Repository(
                name=repo_name_full,
                format_=artifactregistry_v1.Repository.Format.DOCKER,
                description="Repository for SkyPilot serverless deployments.",
            )
            create_op = ar_client.create_repository(
                parent=repo_parent, repository=repository, repository_id=repo_id
            )
            create_op.result()  # Wait
            _update_status(f"Successfully created repository '{repo_id}'.")

    def _build_from_source(self, gcs_source_uri: str, image_name: str) -> str:
        """Triggers a Cloud Build job and waits for it to complete."""
        build_client = build_v1.CloudBuildClient()
        if not gcs_source_uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI: {gcs_source_uri}")
        bucket_name, object_path = gcs_source_uri[5:].split("/", 1)

        build = build_v1.Build(
            source=build_v1.Source(
                storage_source=build_v1.StorageSource(
                    bucket=bucket_name, object_=object_path
                )
            ),
            steps=[
                build_v1.BuildStep(
                    name="gcr.io/k8s-skaffold/pack",
                    args=[
                        "build",
                        image_name,
                        "--builder",
                        "gcr.io/buildpacks/builder",
                    ],
                )
            ],
            images=[image_name],
        )

        request = build_v1.CreateBuildRequest(project_id=self.project_id, build=build)
        operation = build_client.create_build(request=request)
        result = operation.result()  # This is the long blocking call

        if result.status != build_v1.Build.Status.SUCCESS:
            raise serverless_exceptions.DeploymentError(
                f"Cloud Build failed. Check logs at: {result.log_url}"
            )
        return image_name

    def _deploy_to_cloud_run(
        self,
        func: data_models.ServerlessFunction,
        region: str,
        image_uri: str,
        service_account_email: str,
    ) -> operation.Operation:
        """
        Deploys or updates a Cloud Run service, handling the different requirements
        for create and update operations.
        """
        run_client = run_v2.ServicesClient()
        service_name_full = (
            f"projects/{self.project_id}/locations/{region}/services/{func.name}"
        )
        resource_limits = {'memory': f'{func.memory_mb}Mi'}
        resource_requirements = run_v2.ResourceRequirements(limits=resource_limits)
        container = run_v2.Container(
            image=image_uri,
            env=[run_v2.EnvVar(name=k, value=v) for k, v in func.env.items()],
            resources=resource_requirements,
        )
        timeout_delta = datetime.timedelta(seconds=func.timeout_seconds)
        template = run_v2.RevisionTemplate(
            containers=[container],
            service_account=service_account_email,
            timeout=timeout_delta,
            max_instance_request_concurrency=1,
        )

        try:
            run_client.get_service(name=service_name_full)

            service_config_for_update = run_v2.Service(
                name=service_name_full,
                template=template,
            )
            update_mask = field_mask_pb2.FieldMask(paths=['template'])
            op = run_client.update_service(
                service=service_config_for_update,
                update_mask=update_mask,
            )
        except google_exceptions.NotFound:
            # If it doesn't exist, create a service config WITHOUT a name.
            service_config_for_create = run_v2.Service(
                # The 'name' field is intentionally left EMPTY as required by the API.
                template=template,
            )
            parent = f"projects/{self.project_id}/locations/{region}"
            op = run_client.create_service(
                parent=parent,
                service=service_config_for_create,  # Use the name-less config.
                service_id=func.name,  # Provide the name here instead.
            )

        return op

    def deploy(
        self,
        func: data_models.ServerlessFunction,
        triggers: List[data_models.Trigger],
        *,
        region: str,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """Deploys a serverless function with content-aware image caching."""

        def _update_status(message: str):
            if status_callback:
                status_callback(message)

        source_dir = None
        try:
            _update_status("Checking required GCP APIs...")
            self._check_apis_enabled()

            _update_status("Setting up IAM for Guardian...")
            service_account_email = self._setup_iam_for_guardian(region)

            _update_status("Preparing and analyzing source directory...")
            source_dir = self._prepare_source_for_deployment(func)

            source_hash = self._compute_source_hash(source_dir)
            _update_status(f"Source code hash: {source_hash[:12]}...")

            repo_id = os.environ.get(
                "SKYPILOT_GCP_AR_REPOSITORY", "cloud-run-source-deploy"
            )
            self._ensure_artifact_registry_repo_exists(region, repo_id, status_callback)

            docker_image_uri = (
                f"{region}-docker.pkg.dev/{self.project_id}/{repo_id}/"
                f"{func.name}:{source_hash}"
            )
            api_image_name_by_digest = (
                f"projects/{self.project_id}/locations/{region}/repositories/{repo_id}/"
                f"dockerImages/{func.name}@sha256:{source_hash}"
            )

            image_uri = None
            ar_client = artifactregistry_v1.ArtifactRegistryClient()
            try:
                _update_status("Checking for existing container image...")
                ar_client.get_docker_image(name=api_image_name_by_digest)

                _update_status("Cache hit! Reusing existing image.")
                image_uri = docker_image_uri
            except NotFound:
                _update_status("Cache miss. A new build is required.")

                _update_status("Uploading source to GCS...")
                gcs_source_uri = self._upload_source_to_gcs(source_dir, region)

                _update_status("Building container (this may take a few minutes)...")
                image_uri = self._build_from_source(gcs_source_uri, docker_image_uri)

            _update_status("Deploying to Cloud Run...")
            deploy_op = self._deploy_to_cloud_run(
                func, region, image_uri, service_account_email
            )
            deployed_service = deploy_op.result()

            _update_status("Configuring triggers...")
            for trigger in triggers:
                if isinstance(trigger, data_models.CronTrigger):
                    job_id = f"{func.name}-job"
                    job_name = self.scheduler_client.job_path(
                        self.project_id, region, job_id
                    )

                    http_target = scheduler_v1.HttpTarget(
                        uri=deployed_service.uri,
                        http_method=scheduler_v1.HttpMethod.POST,
                        # Set an empty JSON body to make it a valid POST request.
                        body=b'{}',
                        # Explicitly set the Content-Type header.
                        headers={'Content-Type': 'application/json'},
                        # OIDC token for authentication remains the same.
                        oidc_token=scheduler_v1.OidcToken(
                            service_account_email=service_account_email,
                            audience=deployed_service.uri
                        ),
                    )

                    job = scheduler_v1.Job(
                        name=job_name,
                        description=f"Cron trigger for {func.name}",
                        schedule=trigger.schedule,
                        time_zone="Etc/UTC",
                        http_target=http_target,
                    )

                    try:
                        self.scheduler_client.create_job(
                            parent=f"projects/{self.project_id}/locations/{region}",
                            job=job,
                        )
                        _update_status(f"Created cron job '{job_id}'.")
                    except google_exceptions.AlreadyExists:
                        self.scheduler_client.update_job(job=job)
                        _update_status(f"Updated existing cron job '{job_id}'.")
                else:
                    _update_status(
                        f"Skipping unsupported trigger type: {type(trigger).__name__}"
                    )

            _update_status(f"Service '{func.name}' deployed successfully.")
            return {"uri": deployed_service.uri}

        except Exception as e:
            raise e
        finally:
            if source_dir and os.path.exists(source_dir):
                shutil.rmtree(source_dir)

    def delete(self, name: str, *, region: str) -> None:
        """Deletes a serverless function and its triggers from GCP.

        This version is updated to correctly delete Cloud Run services and their
        associated trigger resources.
        """
        run_client = run_v2.ServicesClient()
        service_name_full = (
            f"projects/{self.project_id}/locations/{region}/services/{name}"
        )
        try:
            print(f"Attempting to delete Cloud Run service: {name}...")
            delete_op = run_client.delete_service(name=service_name_full)
            delete_op.result()
            print(f"Cloud Run service '{name}' deleted successfully.")
        except google_exceptions.NotFound:
            print(f"Cloud Run service '{name}' not found, skipping.")
        except Exception as e:
            print(
                f"{colorama.Fore.RED}Error deleting Cloud Run service '{name}': {e}{colorama.Style.RESET_ALL}"
            )

        scheduler_job_id = f"{name}-job"
        scheduler_job_name = self.scheduler_client.job_path(
            self.project_id, region, scheduler_job_id
        )
        try:
            print(f"Attempting to delete Cloud Scheduler job: {scheduler_job_id}...")
            self.scheduler_client.delete_job(name=scheduler_job_name)
            print(f"Scheduler job '{scheduler_job_id}' deleted.")
        except google_exceptions.NotFound:
            print(f"Scheduler job '{scheduler_job_id}' not found, skipping.")

    def logs(self, name: str, *, region: str, follow: bool = True) -> None:
        """Streams logs for a serverless function from GCP Cloud Logging."""
        raise NotImplementedError
