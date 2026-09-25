# -*- coding: utf-8 -*-
"""地址工具：host:port 的格式化与解析（IPv4 / IPv6 双栈）。"""


def fmt_host_port(ip, port):
    """格式化 'ip:port'；IPv6 用 '[ip]:port' 以便区分端口。"""
    if ":" in ip:
        return "[%s]:%d" % (ip, port)
    return "%s:%d" % (ip, port)


def parse_host_port(s):
    """解析 'ip:port' 或 '[ipv6]:port'，返回 (ip, port) 或 None。"""
    if not s:
        return None
    s = s.strip()
    if s.startswith("["):
        close = s.find("]")
        if close < 0:
            return None
        ip = s[1:close]
        rest = s[close + 1:]
        if not rest.startswith(":"):
            return None
        try:
            return (ip, int(rest[1:]))
        except ValueError:
            return None
    if ":" not in s:
        return None
    ip, port = s.rsplit(":", 1)
    if ":" in ip:            # 未加方括号的 IPv6，非法
        return None
    try:
        return (ip, int(port))
    except ValueError:
        return None
