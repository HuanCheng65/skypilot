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
"""User-facing APIs for SkyPilot Serverless."""
from typing import Dict, List, Optional, Callable

from sky.clouds import cloud as cloud_lib
from sky.serverless.backends import registry
from sky.serverless import data_models


def deploy(
    func: data_models.ServerlessFunction,
    triggers: List[data_models.Trigger],
    *,
    cloud: cloud_lib.Cloud,
    region: str,
    status_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, str]:
    """Deploys a serverless function to the specified cloud.

    This high-level API automatically selects the appropriate backend based
    on the `cloud` parameter and delegates the deployment task to it.

    Args:
        func: The ServerlessFunction object to deploy.
        triggers: A list of Trigger objects to attach to the function.
        cloud: The cloud provider to deploy to.
        region: The cloud region for the deployment.

    Returns:
        A dictionary of deployment outputs from the backend.
    """
    backend = registry.get_backend(cloud)
    return backend.deploy(func, triggers, region=region, status_callback=status_callback)


def delete(name: str, *, cloud: cloud_lib.Cloud, region: str) -> None:
    """Deletes a serverless function from the specified cloud.

    Args:
        name: The name of the function to delete.
        cloud: The cloud provider to delete from.
        region: The cloud region where the function is located.
    """
    backend = registry.get_backend(cloud)
    backend.delete(name, region=region)


def logs(
    name: str,
    *,
    cloud: cloud_lib.Cloud,
    region: str,
    follow: bool = True,
) -> None:
    """Retrieves or streams logs for a serverless function.

    Args:
        name: The name of the function.
        cloud: The cloud provider.
        region: The cloud region.
        follow: Whether to stream logs continuously.
    """
    backend = registry.get_backend(cloud)
    backend.logs(name, region=region, follow=follow) 