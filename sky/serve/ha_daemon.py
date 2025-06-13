# sky/serve/ha_daemon.py
import argparse
import os
import signal
import subprocess
import sys
import threading
import time

import psutil
import requests
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from sky import sky_logging
from sky.serve import serve_utils

logger = sky_logging.init_logger("sky.serve.ha_daemon")
app = FastAPI()

# --- 全局变量 ---
SERVICE_NAME: str = ""
TASK_YAML_PATH: str = ""
JOB_ID: int = -1
CONTROLLER_PORT: int = -1

RESTART_LOCK = threading.Lock()


def find_service_process() -> psutil.Process | None:
    # 这个函数保持不变
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info["cmdline"]
            if (
                cmdline
                and "python" in cmdline[0]
                and "sky.serve.service" in cmdline
                and "--service-name" in cmdline
                and SERVICE_NAME in cmdline
            ):
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return None


def restart_service_process():
    # 这个函数保持不变，包括日志重定向
    if not RESTART_LOCK.acquire(blocking=False):
        logger.warning("Restart already in progress. Ignoring.")
        return
    try:
        logger.info("Initiating service restart with log redirection...")
        old_process = find_service_process()
        restart_command: list[str] = [
            sys.executable,
            "-m",
            "sky.serve.service",
            "--service-name",
            SERVICE_NAME,
            "--task-yaml",
            TASK_YAML_PATH,
            "--job-id",
            str(JOB_ID),
        ]
        log_file_path = os.path.expanduser(
            serve_utils.generate_remote_controller_log_file_name(SERVICE_NAME)
        )
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        with open(log_file_path, "a") as log_f:
            subprocess.Popen(
                restart_command,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        time.sleep(2)
        if old_process:
            logger.info(f"Terminating old service process (PID: {old_process.pid})...")
            os.kill(old_process.pid, signal.SIGKILL)
    finally:
        RESTART_LOCK.release()


# ======================= 恢复为两个独立的端点 =======================
@app.get("/healthz")
def health_check():
    """
    代理对本地 Controller 的健康检查。
    无论成功或失败，都返回统一格式的 JSON。
    """
    try:
        health_url = f"http://127.0.0.1:{CONTROLLER_PORT}/controller/healthz"
        response = requests.get(health_url, timeout=2)

        # 如果 Controller 返回非 200，我们也认为是不健康的
        if response.status_code == 200:
            # 直接透传 Controller 的健康响应
            return JSONResponse(content=response.json(), status_code=200)
        else:
            reason = f"Controller service responded with non-200 status: {response.status_code}"
            logger.warning(f"Health check failed: {reason}")
            return JSONResponse(
                content={"status": "unhealthy", "reason": reason}, status_code=503
            )

    except requests.exceptions.RequestException as e:
        reason = f"Failed to connect to local controller: {e}"
        logger.warning(f"Health check failed: {reason}")
        return JSONResponse(
            content={"status": "unhealthy", "reason": reason},
            status_code=503,  # Service Unavailable
        )


@app.post("/restart")
def trigger_restart():
    """
    外部触发此端点来执行重启操作。
    """
    logger.info("Received external request to restart the service.")
    restart_service_process()
    return JSONResponse(
        content={
            "status": "restart_initiated",
            "message": "Restart command has been issued.",
        },
        status_code=202,  # Accepted
    )


# ... main 函数和参数解析保持不变 ...
def run_daemon_main(
    service_name: str,
    task_yaml: str,
    job_id: int,
    controller_port: int,
    daemon_port: int,
):
    global SERVICE_NAME, TASK_YAML_PATH, JOB_ID, CONTROLLER_PORT
    SERVICE_NAME = service_name
    TASK_YAML_PATH = task_yaml
    JOB_ID = job_id
    CONTROLLER_PORT = controller_port
    uvicorn.run(app, host="0.0.0.0", port=daemon_port, log_level="info")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SkyServe HA Daemon")
    parser.add_argument("--service-name", required=True)
    parser.add_argument("--task-yaml", required=True)
    parser.add_argument("--job-id", type=int, required=True)
    parser.add_argument("--controller-port", type=int, required=True)
    parser.add_argument("--daemon-port", type=int, default=9001)
    args = parser.parse_args()
    run_daemon_main(
        args.service_name,
        args.task_yaml,
        args.job_id,
        args.controller_port,
        args.daemon_port,
    )
