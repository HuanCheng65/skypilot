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
"""Core data structures for serverless functions."""
import dataclasses
from typing import Dict, List, Union, Literal


# --- Trigger Definitions (Type-Safe) ---
# We use separate dataclasses for each trigger type and combine them with a
# Union. This allows IDEs and type checkers like MyPy to validate parameters
# at creation time, preventing common errors associated with unstructured
# dictionaries.


@dataclasses.dataclass(frozen=True)
class CronTrigger:
    """Defines a cron-based trigger for a serverless function.

    Attributes:
        schedule: A cron-like expression. The specific syntax is translated
            by each cloud backend to its supported format.
            E.g., '*/5 * * * *' for every 5 minutes.
    """
    schedule: str


@dataclasses.dataclass(frozen=True)
class HttpTrigger:
    """Defines an HTTP endpoint trigger.

    Attributes:
        port: The listening port.
        method: The allowed HTTP method (e.g., 'GET', 'POST').
    """
    port: int = 8080
    method: str = 'POST'
    # Future extensions: auth_method, cors_config, etc.


# A Union of all supported trigger types. Functions accepting triggers should
# use this type hint for clarity and type safety.
Trigger = Union[CronTrigger, HttpTrigger]


# --- ServerlessFunction Definition ---


@dataclasses.dataclass
class ServerlessFunction:
    """A platform-agnostic description of a serverless function.

    Attributes:
        name: The unique name of the function, mapped to the cloud resource name.
        code_path: Path to the local directory containing the function's source.
        entrypoint: The function entrypoint, in 'filename.function_name' format.
        env: Environment variables to be injected into the function's runtime.
        runtime: The code runtime environment, e.g., 'python311'.
        memory_mb: Memory allocated to the function in megabytes.
        timeout_seconds: The function's execution timeout.
    """
    name: str
    code_path: str
    entrypoint: str
    env: Dict[str, str]

    # Runtime configuration
    runtime: str = 'python311'
    memory_mb: int = 512
    timeout_seconds: int = 300
    extra_requirements: List[str] = dataclasses.field(default_factory=list)

    # Ingress settings
    # 'internal-only': For triggers like Pub/Sub, Cron jobs.
    # 'all': For public-facing triggers like HTTP.
    ingress: Literal['internal-only', 'all'] = 'internal-only'