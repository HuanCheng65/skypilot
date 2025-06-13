# sky/serve/launch_daemon.py
import os
import sys
import time

def daemonize():
    """
    执行标准的双重 fork，将当前进程变为一个完全独立的守护进程。
    """
    # 第一次 fork
    try:
        pid = os.fork()
        if pid > 0:
            # 父进程直接退出
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"fork #1 failed: {e}\n")
        sys.exit(1)

    # 从父进程环境中分离
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # 第二次 fork
    try:
        pid = os.fork()
        if pid > 0:
            # 第二个父进程（第一个子进程）也退出
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"fork #2 failed: {e}\n")
        sys.exit(1)

    # --- 现在我们处于一个完全分离的孙子进程中 ---

    # 重定向标准文件描述符
    sys.stdout.flush()
    sys.stderr.flush()
    si = open(os.devnull, 'r')
    so = open(os.devnull, 'a+')
    se = open(os.devnull, 'a+')
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

if __name__ == '__main__':
    # 变成守护进程
    daemonize()

    # 等待一秒确保父进程有足够时间退出
    time.sleep(1)

    # 用 ha_daemon 的逻辑替换当前进程
    # sys.argv[0] 是 'launch_daemon.py'
    # sys.argv[1] 是 'python'
    # sys.argv[2] 是 '-m'
    # sys.argv[3] 是 'sky.serve.ha_daemon'
    # ...以此类推
    # 我们需要从 sys.argv[1] 开始构造命令
    
    args_for_daemon = sys.argv[1:]
    
    # os.execv 需要可执行文件的完整路径和参数列表
    python_executable = args_for_daemon[0]
    # execv的第一个参数是程序名本身
    os.execv(python_executable, args_for_daemon)