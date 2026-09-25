# -*- coding: utf-8 -*-
"""DPMP 应用默认值（非协议常量）。

协议常量见 dpmp.protocol.constants（与对端约定，不可配）。
本模块放**部署相关的默认值**——默认信令服务器等，方便开箱即用。
这些值随时可被使用者的参数覆盖，且**不应被协议层依赖**。

⚠️ 默认服务器是「便利」，不是「依赖」：
    · 到期后客户端会明确提示，使用者应改用自建/备用服务器；
    · 通过 SignalingClient(servers=[...]) 可完全绕过默认值；
    · 局域网模式完全不使用服务器。
"""

import datetime
from typing import Dict, List, Optional, Tuple

from .protocol import constants as C

# ============================================================
# 默认信令服务器（便利入口，非强制）
# ============================================================
DEFAULT_SERVER: str = "42.194.133.132"
DEFAULT_SERVER_PORT: int = C.DEFAULT_SERVER_PORT            # 3336
DEFAULT_SERVER_TCP_PORT: int = C.DEFAULT_SERVER_TCP_PORT    # 3337
DEFAULT_NAT_PROBE_PORT: int = C.NAT_PROBE_PORT              # 3338

# 默认服务器到期日（ISO 日期）。到期/临近时客户端会提示。
# 使用者应在此日期后改用自建服务器或备用服务器。
DEFAULT_SERVER_EXPIRES: str = "2026-11-01"

# 临近到期的提醒阈值（天）
EXPIRY_WARN_DAYS: int = 7


def default_servers() -> List[Tuple[str, int, int, int]]:
    """返回默认服务器候选列表（单个）。

    每项为 (ip, 信令端口, TCP 映射观测端口, NAT 探测端口)。
    """
    return [(DEFAULT_SERVER, DEFAULT_SERVER_PORT,
             DEFAULT_SERVER_TCP_PORT, DEFAULT_NAT_PROBE_PORT)]


def check_default_server_expiry(
        today: Optional[datetime.date] = None) -> Dict[str, object]:
    """检查默认服务器的到期状态。

    返回 dict：
      {"status": "ok"|"soon"|"expired"|"unknown",
       "days_left": int|None,
       "expires": "YYYY-MM-DD"}
    """
    if today is None:
        today = datetime.date.today()
    try:
        exp = datetime.date.fromisoformat(DEFAULT_SERVER_EXPIRES)
    except Exception:
        return {"status": "unknown", "days_left": None,
                "expires": DEFAULT_SERVER_EXPIRES}
    days_left = (exp - today).days
    if days_left < 0:
        status = "expired"
    elif days_left <= EXPIRY_WARN_DAYS:
        status = "soon"
    else:
        status = "ok"
    return {"status": status, "days_left": days_left,
            "expires": DEFAULT_SERVER_EXPIRES}
