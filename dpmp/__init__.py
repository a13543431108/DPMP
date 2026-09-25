# -*- coding: utf-8 -*-
"""DPMP — Dual-Punch Multi-Path Protocol.

P2P 连接与传输底层库。

对外分层：
    dpmp.protocol   协议常量与编解码
    dpmp.link       连接层（发现 / 信令 / 双打洞 / UDP-RTP / 路径调度 / 连接管理）
    dpmp.stream     传输层（统一字节流通道 + 多径选路）
    dpmp.util       工具层（网络探测 / 设备标识）

典型用法（远程控制 / 投屏 / 游戏房间通用）：

    from dpmp.link.manager import LinkManager
    from dpmp.link.signaling import SignalingClient

    lm = LinkManager(sig, local_tcp_port=9998)
    lm.on_socket_ready = lambda pid, sock, m: start_my_protocol(sock)
    # 之后用 lm.get_send_channel(pid) 拿到可用通道发送字节流
"""

from .config import Config, DEFAULT_CONFIG
from .defaults import DEFAULT_SERVER, default_servers

__version__: str = "0.1.3"
__protocol__: str = "DPMP/1.0"

__all__ = [
    "Config", "DEFAULT_CONFIG",
    "DEFAULT_SERVER", "default_servers",
]
