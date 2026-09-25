# -*- coding: utf-8 -*-
"""信令服务器 —— 跨平台启动器（带崩溃自动重启）。

用法：
    python 启动.py                       # 前台运行（崩溃自动重启）
    python 启动.py --port 4000           # 透传参数给服务器
    python 启动.py --no-restart          # 关闭自动重启
    python 启动.py --restart-delay 5     # 崩溃后等待秒数

特性：
    · 崩溃自动重启（异常退出后自动拉起；正常退出/Ctrl+C 不重启）
    · 房间状态持久化（由服务器自身完成，重启后恢复上次房间）
    · 环境检查（Python 版本）
    · 参数透传
"""

import os
import subprocess
import sys
import time


MIN_PY = (3, 7)


def _check_python():
    if sys.version_info < MIN_PY:
        print("[错误] 需要 Python %d.%d+，当前 %s"
              % (MIN_PY[0], MIN_PY[1], sys.version.split()[0]))
        sys.exit(1)


def main():
    _check_python()

    here = os.path.dirname(os.path.abspath(__file__))
    target = os.path.join(here, "信令服务器.py")
    if not os.path.isfile(target):
        print("[错误] 找不到 %s" % target)
        sys.exit(1)

    # 解析本启动器自己的选项
    args = sys.argv[1:]
    no_restart = "--no-restart" in args
    args = [a for a in args if a != "--no-restart"]

    restart_delay = 3
    for i, a in enumerate(args):
        if a == "--restart-delay" and i + 1 < len(args):
            try:
                restart_delay = int(args[i + 1])
            except ValueError:
                pass
            args = args[:i] + args[i + 2:]
            break
        elif a.startswith("--restart-delay="):
            try:
                restart_delay = int(a.split("=", 1)[1])
            except ValueError:
                pass
            args = args[:i] + args[i + 1:]
            break

    print("=" * 60)
    print("信令服务器启动器")
    print("  自动重启: %s" % ("关闭" if no_restart else "开启（崩溃后 %ds 重启）" % restart_delay))
    print("  房间状态: 持久化到 server_state.json（重启后恢复）")
    print("=" * 60)

    crash_count = 0
    while True:
        cmd = [sys.executable, "-u", target] + args
        try:
            rc = subprocess.call(cmd)
        except KeyboardInterrupt:
            print("\n[退出] 收到 Ctrl+C，服务器已停止")
            return

        # rc == 0：服务器正常退出（如手动停止）
        if rc == 0 or no_restart:
            print("[退出] 服务器正常结束（返回码 %d）" % rc)
            return

        # 异常退出：自动重启
        crash_count += 1
        print("[重启] 服务器异常退出（返回码 %d），%d 秒后自动重启（第 %d 次）"
              % (rc, restart_delay, crash_count))
        try:
            time.sleep(restart_delay)
        except KeyboardInterrupt:
            print("\n[退出] 收到 Ctrl+C，取消重启")
            return


if __name__ == "__main__":
    main()
