# -*- coding: utf-8 -*-
"""DPMP 连接层。

负责「找到对端 + 建立可用通道」：
    discovery   局域网 UDP 发现（广播 / 扫描 / 心跳）
    signaling   公网信令客户端（加入房间 / 心跳 / 成员事件）
    puncher     双打洞核心（TCP 同时打开 + UDP 端口预测）
    rtp         UDP 可靠通道（UDP-RTP）
    path        梯度冗余多路径调度
    manager     连接管理（全连接 + 长连接 + 入站接管 + 保活）
"""

from .discovery import Discovery
from .signaling import SignalingClient
from .puncher import HolePuncher, PunchResult
from .rtp import UdpReliableSocket
from .path import PeerPathScheduler, Path, KeepaliveScheduler
from .manager import LinkManager, Conn

__all__ = [
    "Discovery",
    "SignalingClient",
    "HolePuncher", "PunchResult",
    "UdpReliableSocket",
    "PeerPathScheduler", "Path", "KeepaliveScheduler",
    "LinkManager", "Conn",
]
