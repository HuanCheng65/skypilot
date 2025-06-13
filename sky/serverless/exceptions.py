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
"""Custom exceptions for the serverless module."""


class ServerlessError(Exception):
    """Base class for serverless-related errors."""
    pass


class ServerlessDeploymentError(ServerlessError):
    """Raised when a serverless deployment fails."""
    pass


class ServerlessDeletionError(ServerlessError):
    """Raised when a serverless deletion fails."""
    pass 

class CloudAPIDisabledError(ServerlessError):
    """Raised when a required cloud API is disabled."""
    pass