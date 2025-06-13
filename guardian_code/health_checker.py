# health_checker.py
import os
from typing import Any, Dict, Tuple

import requests
from google.api_core import exceptions as google_exceptions
from google.cloud import compute_v1
from google import auth

# 目标端口是 HA Daemon 的端口
DAEMON_PORT = 9001
# 健康检查端点
DAEMON_HEALTH_CHECK_ENDPOINT = '/healthz'


class HealthChecker:
    """Performs health checks by querying the HA Daemon on the controller VM."""

    # ======================= 已恢复为您原始的、正确的构造函数 =======================
    def __init__(self, service_name: str, service_id: str, cloud: str,
                 region: str):
        self.service_name = service_name
        self.service_id = service_id
        self.cloud = cloud
        self.region = region
        self.project_id = None
        if self.cloud.lower() == 'gcp':
            try:
                _, self.project_id = auth.default()
            except auth.exceptions.DefaultCredentialsError:
                print('CRITICAL: Could not automatically determine GCP project ID.')
    # ==============================================================================

    def check(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Performs a health check via the HA Daemon.
        """
        if self.cloud.lower() != 'gcp':
            print(f'Health check for cloud {self.cloud} not implemented.')
            return True, {}

        if not self.project_id:
            print('Health Check failed: GCP Project ID is not available.')
            return True, {}

        return self._check_gcp()

    # ======================= 已恢复为您原始的、正确的实例查找逻辑 =======================
    def _get_controller_instance_gcp(
            self) -> Tuple[compute_v1.Instance | None, str | None]:
        """Finds the controller instance on GCP."""
        client = compute_v1.InstancesClient()
        filter_str = 'labels.skypilot-head-node="1"'
        try:
            request = compute_v1.AggregatedListInstancesRequest(
                project=self.project_id,
                filter=filter_str,
            )
            agg_list = client.aggregated_list(request=request)

            controller_instances = []
            for _, per_zone_list in agg_list:
                if per_zone_list.instances:
                    for instance in per_zone_list.instances:
                        if instance.name.startswith('sky-serve-controller-'):
                            controller_instances.append(instance)

            if not controller_instances:
                return None, 'Controller instance not found (no instance name starting with "sky-serve-controller-").'
            
            return controller_instances[0], None
        
        except google_exceptions.GoogleAPICallError as e:
            return None, f'GCP API Error: {e}'
    # ==============================================================================

    def _check_gcp(self) -> Tuple[bool, Dict[str, Any]]:
        """Health check implementation for GCP."""
        instance, error_msg = self._get_controller_instance_gcp()
        if error_msg:
            print(f'Health Check failed: {error_msg}')
            return True, {}
        assert instance is not None

        # 1. 检查实例状态和公网IP
        public_ip = None
        if instance.status != 'RUNNING':
            print(f'Health Check: Instance status is {instance.status}. Unhealthy.')
            return False, {"reason": f"Instance status is {instance.status}"}

        try:
            public_ip = instance.network_interfaces[0].access_configs[0].nat_i_p
        except (IndexError, AttributeError):
            print('Health Check: Instance is RUNNING but has no public IP. Unhealthy.')
            return False, {"reason": "Instance has no public IP"}
        
        daemon_base_url = f'http://{public_ip}:{DAEMON_PORT}'
        health_url = f'{daemon_base_url}{DAEMON_HEALTH_CHECK_ENDPOINT}'

        # 2. 检查健康端点
        try:
            response = requests.get(health_url, timeout=5)
            if response.status_code == 200:
                print('Health Check: Controller is RUNNING and daemon is ready. Healthy.')
                return True, {}

            print(f'Health Check: Daemon health probe failed with status '
                  f'{response.status_code}. Unhealthy.')
            return False, {'daemon_url': daemon_base_url, 'reason': response.json().get('reason')}
        except requests.exceptions.RequestException as e:
            print(f'Health Check: Daemon health probe connection failed: {e}. '
                  'Unhealthy.')
            return False, {'daemon_url': daemon_base_url, 'reason': f'Failed to connect to daemon: {e}'}

    # 不再需要 _collect_recovery_info_gcp 方法