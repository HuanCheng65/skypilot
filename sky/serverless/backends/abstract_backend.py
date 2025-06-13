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
"""Abstract interface for serverless backend implementations."""
from typing import Dict, List, Protocol, Optional, Callable

from sky.serverless import data_models


class AbstractServerlessBackend(Protocol):
    """Abstract interface for a serverless backend implementation.

    Each cloud that supports serverless functions must provide a class that
    implements this protocol.
    """

    def deploy(
        self,
        func: data_models.ServerlessFunction,
        triggers: List[data_models.Trigger],
        *,
        region: str,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, str]:
        """Deploys a serverless function and its triggers.

        This method handles all cloud-specific logic, including:
        1.  Code Packaging: Bundling the source code and dependencies from
            `func.code_path` into a deployment package (e.g., a .zip file).
        2.  IAM Setup: Creating or updating the necessary IAM roles and
            attaching minimal permissions for the function to execute.
        3.  Code Upload: Uploading the packaged code to a cloud storage
            service (e.g., S3, GCS Bucket).
        4.  Function Create/Update: Creating or updating the function resource
            on the cloud platform.
        5.  Trigger Configuration: Setting up and binding the corresponding
            cloud resources for each trigger (e.g., EventBridge Rule,
            API Gateway, Cloud Scheduler).

        Returns:
            A dictionary containing information about the deployed resources,
            such as {'function_arn': '...', 'http_url': '...'}.
        """
        raise NotImplementedError

    def delete(self, name: str, *, region: str) -> None:
        """Deletes a serverless function and all associated resources.

        Implementations must ensure that all related resources, such as IAM
        roles and triggers, are properly cleaned up to avoid orphans.
        """
        raise NotImplementedError

    def logs(self, name: str, *, region: str, follow: bool = True) -> None:
        """Streams or retrieves logs for a specified serverless function.

        Args:
            name: The name of the function to fetch logs for.
            region: The cloud region where the function is deployed.
            follow: If True, streams logs continuously (like `tail -f`).
                If False, retrieves available logs and exits.
        """
        raise NotImplementedError 