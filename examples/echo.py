# -*- coding: utf-8 -*-
"""DPMP 端到端示例：两台设备通过「房间号」互发消息。

这是最贴近真实用法的例子——把 DPMP 当作一条 P2P 字节流通道，
两端各自跑本脚本，加入同一房间后即可互相收发文本消息。

用法：
    # 需要一台已部署的 DPMP 信令服务器（见项目 server/ 目录）
    # 设备 A（创建受保护房间）：
    python echo.py --server 1.2.3.4 --room myroom --pwd s3cret --name Alice
    # 设备 B（另一台机器，须密码一致）：
    python echo.py --server 1.2.3.4 --room myroom --pwd s3cret --name Bob

    # 不带 --pwd = 开放房间，任何人凭房间号直接进：
    python echo.py --server 1.2.3.4 --room myroom --name Alice

    # 不做参数时使用内置默认服务器
    python echo.py --room myroom --name Alice

运行时：
    · 直接输入文字并回车 → 广播给房间内所有已连接的对端
    · 收到对端消息 → 自动打印，并回一句 "echo: <原消息>"
    · 输入 /quit 退出

说明：
    · 本示例用「房间模式」（需要信令服务器牵线 + 打洞）。
    · 连接建立后，消息在两端之间【直连】传输，不经服务器。
    · 通道可能是 TCP，也可能是 UDP-RTP（取决于打洞结果），
      对上层透明——统一用 lm.get_send_channel() 取可用通道。
"""
import argparse
import sys
import threading
import time

from dpmp.link.signaling import SignalingClient
from dpmp.link.manager import LinkManager
from dpmp.util.net import get_or_create_device_id


def main():
    ap = argparse.ArgumentParser(description="DPMP 端到端消息示例")
    ap.add_argument("--server", default=None,
                    help="信令服务器地址（不填则用内置默认服务器）")
    ap.add_argument("--port", type=int, default=3336, help="信令端口（默认 3336）")
    ap.add_argument("--room", required=True, help="房间号（两端填同一个）")
    ap.add_argument("--pwd", default="", help="房间密码（可选；不填=开放房间，谁都能进）")
    ap.add_argument("--name", required=True, help="显示名")
    ap.add_argument("--tcp-port", type=int, default=9998, help="打洞/长连接 TCP 端口")
    ap.add_argument("--udp-hole-port", type=int, default=9996, help="UDP 打洞端口")
    args = ap.parse_args()

    did = get_or_create_device_id("~/.dpmp_config.json")

    # ---- ① 信令客户端（负责找对端 + 协调打洞）----
    sig_kwargs = dict(
        room=args.room,
        name=args.name,
        tcp_port=args.tcp_port,
        punch_local_port=args.tcp_port,
        udp_hole_port=args.udp_hole_port,
        device_id=did,
        # 房间密码：空 = 开放房间（任何人凭房间号可进）；填了 = 受保护房间。
        # 首位加入者决定房间性质；后续加入者密码不匹配会被服务器拒绝。
        password=args.pwd,
        log=lambda m: print("[信令]", m),
    )
    if args.server:
        sig_kwargs["server_ip"] = args.server
        sig_kwargs["server_port"] = args.port
    sig = SignalingClient(**sig_kwargs)

    # ---- ② 连接管理（打洞 + 多路径 + 保活 + 重连）----
    lm = LinkManager(sig, local_tcp_port=args.tcp_port,
                     log=lambda m: print("[连接]", m))

    # ---- ③ 【必须】把信令事件接到连接管理 ----
    sig.on_joined         = lm.on_joined
    sig.on_member_join    = lm.on_member_join
    sig.on_member_leave   = lm.on_member_leave
    sig.on_punch_go       = lm.on_punch_go
    sig.on_udp_hole_ready = lm.on_udp_hole_ready
    sig.on_mapping_ready  = lm.mark_mapping_ready

    # ---- ④ 连接就绪：启动接收循环 ----
    def on_ready(peer_id, sock, member):
        who = member.get("name", peer_id)
        print("[+] 已连接: %s (%s)" % (who, peer_id))
        threading.Thread(target=recv_loop, args=(peer_id, who),
                         daemon=True).start()

    lm.on_socket_ready = on_ready

    def recv_loop(peer_id, who):
        """循环接收该对端发来的消息，并回显。"""
        while lm.has_peer(peer_id):
            ch = lm.get_send_channel(peer_id)
            if ch is None:
                time.sleep(0.5)
                continue
            sock, lock, is_udp, role = ch
            try:
                data = sock.recv(4096)          # 最多读 4096 字节
            except Exception:
                time.sleep(0.2)
                continue
            if not data:
                time.sleep(0.2)
                continue
            text = data.decode("utf-8", "replace").strip()
            print("\n[<] %s: %s" % (who, text))
            # 回显
            try:
                with lock:
                    sock.sendall(("echo: " + text).encode("utf-8"))
            except Exception as e:
                print("[!] 回显失败:", e)

    # ---- ⑤ 网络切换时重建（宿主检测到后调用）----
    # 本示例未做平台检测；真实项目里在检测到 Wi-Fi/蜂窝切换时调用：
    #     lm.on_network_changed()

    # ---- ⑥ 启动 ----
    lm.start()
    if not sig.start():
        print("[错误] 无法连接信令服务器")
        sys.exit(1)
    print("[*] 已加入房间 %s，等待对端...（输入文字回车发送，/quit 退出）"
          % args.room)

    # ---- ⑦ 主循环：从 stdin 读消息，广播给所有对端 ----
    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            text = line.strip()
            if not text:
                continue
            if text == "/quit":
                break
            members = lm.get_members()
            if not members:
                print("[!] 暂无已连接对端")
                continue
            for m in members:
                pid = m.get("id")
                if not pid or m.get("state") != "connected":
                    continue
                ch = lm.get_send_channel(pid)
                if ch is None:
                    continue
                sock, lock, is_udp, role = ch
                try:
                    with lock:
                        sock.sendall(text.encode("utf-8"))
                    print("[>] -> %s: %s" % (m.get("name", pid), text))
                except Exception as e:
                    print("[!] 发送失败:", e)
    except KeyboardInterrupt:
        pass
    finally:
        sig.stop()
        lm.stop()
        print("[*] 已退出")


if __name__ == "__main__":
    main()
