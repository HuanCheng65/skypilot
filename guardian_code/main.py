# guardian_code/main.py

import os
import sys
import logging
import requests  # 新增 import

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
# 不再需要 import sky
from health_checker import HealthChecker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)

app = FastAPI()


@app.post("/")
async def trigger_health_check(request: Request):
    """
    此端点由云调度器通过 HTTP POST 请求触发。
    它执行健康检查，并在需要时通过调用 HA Daemon 来触发单机重启。
    """
    try:
        logger.info(f"Received health check request. Headers: {request.headers}")

        service_name = os.environ.get("SKYPILOT_SERVICE_NAME")
        service_id = os.environ.get("SKYPILOT_SERVICE_ID")
        cloud = os.environ.get("SKYPILOT_CLOUD")
        region = os.environ.get("SKYPILOT_REGION")

        if not all([service_name, service_id, cloud, region]):
            message = "CRITICAL: Missing one or more required environment variables."
            logger.critical(message)
            return JSONResponse(
                content={"status": "error", "reason": "Configuration incomplete"},
                status_code=500,
            )

        logger.info(f"Guardian: Checking health for service '{service_name}'...")
        checker = HealthChecker(service_name, service_id, cloud, region)
        is_healthy, check_result = checker.check()

        if is_healthy:
            logger.info(f"Guardian: Controller for '{service_name}' is healthy.")
            return {"status": "healthy"}

        # ======================= MODIFIED: 核心恢复逻辑 =======================
        logger.warning(
            f"Guardian: Controller for '{service_name}' is UNHEALTHY! "
            "Initiating single-machine restart via daemon..."
        )
        
        daemon_url = check_result.get("daemon_url")
        if not daemon_url:
            logger.critical("Guardian: Cannot recover. Daemon URL not found from health check.")
            return JSONResponse(
                content={"status": "recovery_failed", "reason": "Daemon URL not found"},
                status_code=500,
            )
        
        restart_url = f"{daemon_url}/restart"
        try:
            logger.info(f"Guardian: Sending POST request to {restart_url}")
            response = requests.post(restart_url, timeout=10)
            
            if response.status_code == 202: # 202 Accepted, daemon收到了指令
                logger.info(f"Guardian: Restart successfully initiated via daemon. "
                            f"Daemon response: {response.text}")
                return {"status": "restart_initiated"}
            else:
                logger.error(f"Guardian: Daemon returned a non-success status: "
                             f"{response.status_code}. Response: {response.text}")
                return JSONResponse(
                    content={"status": "recovery_failed", "reason": "Daemon failed to process restart"},
                    status_code=500,
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"Guardian: Failed to send restart request to daemon. Error: {e}")
            return JSONResponse(
                content={"status": "recovery_failed", "reason": f"Failed to connect to daemon: {e}"},
                status_code=500,
            )
        # ========================================================================

    except Exception as e:
        logger.exception(
            f"Guardian: CRITICAL - An unexpected error occurred! Error: {e}"
        )
        return JSONResponse(
            content={"status": "error", "reason": str(e)}, status_code=500
        )