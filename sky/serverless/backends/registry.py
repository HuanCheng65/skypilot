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
"""Registry for serverless backends."""
from typing import Dict, Type

from sky.utils import ux_utils

from sky.serverless.backends import abstract_backend

_REGISTRY: Dict[str, Type[abstract_backend.AbstractServerlessBackend]] = {
    'gcp': 'sky.serverless.backends.gcp_backend.GCPBackend',
}


def get_backend(
    cloud: 'sky.clouds.Cloud',
) -> abstract_backend.AbstractServerlessBackend:
    """Get a serverless backend for a given cloud."""
    backend_class_path = _REGISTRY.get(cloud.canonical_name())
    if backend_class_path is None:
        with ux_utils.print_exception_no_traceback():
            raise ValueError(
                f'Serverless functions are not supported for {cloud.canonical_name()}.')

    module_path, class_name = backend_class_path.rsplit('.', 1)
    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls() 