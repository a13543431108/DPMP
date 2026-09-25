# -*- coding: utf-8 -*-
"""DPMP 传输层。

在连接层建立的通道之上，提供【统一字节流】抽象：
    · Channel        统一 TCP socket / UDP-RTP 的读写接口
    · select_channel 按路径角色选路（hot → warm_safe → warm_loose）

本层不感知文件、不感知业务，只处理字节流。
"""

from .channel import Channel, is_udp_rtp, wrap_channel

__all__ = ["Channel", "is_udp_rtp", "wrap_channel"]
