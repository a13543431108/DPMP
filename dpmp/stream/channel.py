# -*- coding: utf-8 -*-
"""统一通道抽象：屏蔽 TCP socket 与 UDP-RTP 的差异。

对上提供 sendall / recv / recv_into / recv_exact / close / settimeout，
让业务层（文件互传 / 远程控制 / 投屏）无需区分底层协议。
"""

from typing import Any, Optional


def is_udp_rtp(sock: Any) -> bool:
    """判断一个 socket 是否为 UDP-RTP 通道。"""
    return getattr(sock, "is_udp_rtp", False)


class Channel:
    """统一通道。

    参数：
      sock：底层 socket（TCP socket 或 UdpReliableSocket）
      io_lock：收发串行化锁（TCP 用 RoomConn.io_lock，UDP 用 rtp.io_lock）
      role：当前路径角色（hot / warm_safe / warm_loose / "?"）
    """

    __slots__ = ("sock", "io_lock", "role", "_is_udp")

    def __init__(self, sock: Any, io_lock: Any, role: str = "?") -> None:
        self.sock = sock
        self.io_lock = io_lock
        self.role = role
        self._is_udp = is_udp_rtp(sock)

    @property
    def is_udp(self) -> bool:
        return self._is_udp

    def sendall(self, data: bytes) -> Any:
        return self.sock.sendall(data)

    def recv(self, n: int) -> bytes:
        return self.sock.recv(n)

    def recv_into(self, buffer: Any, nbytes: int = 0) -> int:
        return self.sock.recv_into(buffer, nbytes)

    def recv_exact(self, n: int) -> bytes:
        return self.sock.recv_exact(n)

    def settimeout(self, t: Optional[float]) -> Any:
        return self.sock.settimeout(t)

    def getpeername(self) -> Any:
        return self.sock.getpeername()

    def fileno(self) -> int:
        return self.sock.fileno()

    def close(self) -> Any:
        return self.sock.close()


def wrap_channel(sock: Any, io_lock: Any, role: str = "?") -> Channel:
    """把底层 socket 包装成统一 Channel。"""
    return Channel(sock, io_lock, role)
