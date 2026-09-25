# -*- coding: utf-8 -*-
"""统一通道抽象：屏蔽 TCP socket 与 UDP-RTP 的差异。

对上提供 sendall / recv / recv_into / recv_exact / close / settimeout，
让业务层（文件互传 / 远程控制 / 投屏）无需区分底层协议。
"""


def is_udp_rtp(sock):
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

    def __init__(self, sock, io_lock, role="?"):
        self.sock = sock
        self.io_lock = io_lock
        self.role = role
        self._is_udp = is_udp_rtp(sock)

    @property
    def is_udp(self):
        return self._is_udp

    def sendall(self, data):
        return self.sock.sendall(data)

    def recv(self, n):
        return self.sock.recv(n)

    def recv_into(self, buffer, nbytes=0):
        return self.sock.recv_into(buffer, nbytes)

    def recv_exact(self, n):
        return self.sock.recv_exact(n)

    def settimeout(self, t):
        return self.sock.settimeout(t)

    def getpeername(self):
        return self.sock.getpeername()

    def fileno(self):
        return self.sock.fileno()

    def close(self):
        return self.sock.close()


def wrap_channel(sock, io_lock, role="?"):
    """把底层 socket 包装成统一 Channel。"""
    return Channel(sock, io_lock, role)
