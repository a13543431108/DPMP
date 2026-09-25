# -*- coding: utf-8 -*-
"""NAT 类型探测。

原理：向服务器两个不同的 UDP 端口发探测包，对比服务器观测到的公网映射。
    · 锥形 NAT（Cone）：同一本地端口 -> 任意目标的映射相同
    · 对称 NAT（Symmetric）：不同目标的映射不同
    · 无响应：UDP 被完全封堵
"""

import json
import socket
from typing import Callable, Dict, Optional

from ..protocol import constants as C


def detect_nat_type(server_ip: str,
                    server_port: int = C.DEFAULT_SERVER_PORT,
                    probe_port: int = C.NAT_PROBE_PORT,
                    timeout: float = 1.5,
                    log: Optional[Callable[[str], None]] = None
                    ) -> Dict[str, Optional[str]]:
    """返回 dict：
        {"type": "cone"|"symmetric"|"unknown"|"no_udp",
         "primary": "ip:port"|None, "alt": "ip:port"|None}
    """
    _log = log or (lambda m: None)
    fam = socket.AF_INET6 if ":" in server_ip else socket.AF_INET
    s = None
    try:
        s = socket.socket(fam, socket.SOCK_DGRAM)
        s.settimeout(timeout)
    except Exception as e:
        _log("[NAT探测] 创建 UDP socket 失败: %s" % e)
        return {"type": "unknown", "primary": None, "alt": None}

    def _probe(target_port: int, tag: str) -> bool:
        try:
            payload = json.dumps({"type": C.T_NAT_PROBE, "ver": C.DPMP_VER,
                                  "probe": tag}).encode("utf-8")
            s.sendto(payload, (server_ip, target_port))
            return True
        except Exception as e:
            _log("[NAT探测] 向 %s:%d 发包失败: %s" % (server_ip, target_port, e))
            return False

    replies = {}
    try:
        if _probe(server_port, "primary"):
            try:
                data, _ = s.recvfrom(4096)
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == C.T_NAT_PROBE_REPLY:
                    # 服务器回包字段为 "pub"（服务器观察到的公网 ip:port）
                    replies["primary"] = msg.get("pub")
            except Exception:
                pass
        if _probe(probe_port, "alt"):
            try:
                data, _ = s.recvfrom(4096)
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == C.T_NAT_PROBE_REPLY:
                    replies["alt"] = msg.get("pub")
            except Exception:
                pass
    except Exception as e:
        _log("[NAT探测] 探测异常: %s" % e)
    finally:
        if s:
            try:
                s.close()
            except Exception:
                pass

    primary = replies.get("primary")
    alt = replies.get("alt")
    if primary and alt:
        if primary == alt:
            return {"type": "cone", "primary": primary, "alt": alt}
        return {"type": "symmetric", "primary": primary, "alt": alt}
    if primary or alt:
        return {"type": "unknown", "primary": primary, "alt": alt}
    return {"type": "no_udp", "primary": None, "alt": None}
