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
"""Utility functions for the serverless module."""
import os
import pathlib
import shutil
import subprocess
import tempfile
from typing import Optional

import sky


def package_code(code_path: str,
                 cloud_reqs_path: Optional[str] = None) -> str:
    """Packages source code and dependencies into a zip file.

    This function creates a self-contained deployment package by:
    1.  Copying the user's source code from `code_path`.
    2.  Installing dependencies from requirements files into a dedicated
        'lib' subdirectory to avoid namespace conflicts.
    3.  Copying the entire installed `sky` library into the same 'lib'
        subdirectory.

    Args:
        code_path: Path to the directory containing the function's source code.
        cloud_reqs_path: Optional path to a cloud-specific requirements file
            (e.g., 'requirements-gcp.txt').

    Returns:
        The path to the created .zip file.
    """
    temp_dir = tempfile.mkdtemp()
    
    # 1. Copy user's source code to the root of the temp directory
    shutil.copytree(code_path, temp_dir, dirs_exist_ok=True)

    # 2. Create a dedicated directory for all dependencies
    # This avoids namespace conflicts (e.g., with 'google' packages)
    # and mirrors a more standard site-packages structure.
    lib_dir = os.path.join(temp_dir, 'lib')
    os.makedirs(lib_dir, exist_ok=True)

    # 3. Install dependencies from requirements files into the 'lib' dir
    common_reqs_path = os.path.join(code_path, 'requirements.txt')
    req_files = [common_reqs_path]
    if cloud_reqs_path and os.path.exists(cloud_reqs_path):
        req_files.append(cloud_reqs_path)

    for req_file in req_files:
        print(f'Installing dependencies from {req_file} into {lib_dir}')
        if os.path.exists(req_file):
            subprocess.run([
                'pip', 'install', '-r', req_file,
                '--target', lib_dir,  # <-- Install into 'lib' subdir
                '--disable-pip-version-check'
            ],
                           check=True)
            
    # 4. Install the local sky library into the 'lib' directory as well
    sky_path = pathlib.Path(sky.__file__).parent.parent
    print(f'Installing local sky library from {sky_path} into {lib_dir}')
    subprocess.run([
        'pip', 'install',
        '--target', lib_dir,
        str(sky_path)
    ], check=True)


    # 5. Create zip file
    zip_path = shutil.make_archive(base_name=temp_dir,
                                   format='zip',
                                   root_dir=temp_dir)
    shutil.rmtree(temp_dir)
    return zip_path

# This file will contain helper functions, such as the logic for packaging
# source code and dependencies into a deployable .zip file. 