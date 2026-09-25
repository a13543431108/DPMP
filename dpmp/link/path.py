# -*- coding: utf-8 -*-
"""梯度冗余多路径调度（Gradient Redundant Multi-Path）。

DPMP 的独创点之二：同一 peer 维护多条路径，按角色分级、按需保活、可换路续传。

角色：
    hot         热备，传数据，保活最积极
    warm_safe   保守暖备，不传数据，硬性钳制不超过协议安全上限
    warm_loose  宽松暖备，最后防线，贪心探测到省电极致

保活：软性探测（成功指数增长逼近 NAT 超时边界）+ 硬性下限兜底。
"""

import threading
import time
from typing import Callable, Dict, List, Optional

from ..protocol import constants as C


class KeepaliveScheduler:
    """单条路径的保活间隔调度（软性探测 + 硬性下限）。"""

    def __init__(self, role: str, proto: str, config=None) -> None:
        if config is None:
            from ..config import DEFAULT_CONFIG
            config = DEFAULT_CONFIG
        self.cfg = config
        self.role = role
        self.proto = proto
        self.floor: int = config.proto_floor.get(proto, 15)
        self.safe_cap: int = config.proto_safe_cap.get(proto, 60)
        self.current_interval: int = self.floor * 2
        self.upper_bound: Optional[int] = None
        self.lower_bound: Optional[int] = None
        self.success_count: int = 0
        self.fail_count: int = 0
        self.stable: bool = False

    def _clamp(self, v: int) -> int:
        v = max(v, self.floor)
        if self.role == C.ROLE_WARM_SAFE:
            v = min(v, self.safe_cap)
        else:
            v = min(v, 3600)
        return int(v)

    def on_success(self) -> None:
        self.success_count += 1
        self.upper_bound = self.current_interval
        if self.stable:
            return
        total = self.success_count + self.fail_count
        if total >= 5 and self.fail_count / (total + 1) > 0.05:
            self.stable = True
            return
        c = int(self.current_interval * 2 * self.cfg.role_factor.get(self.role, 1.0))
        if self.lower_bound is not None:
            c = min(c, (self.upper_bound + self.lower_bound) // 2)
            self.stable = True
        self.current_interval = self._clamp(c)

    def on_failure(self) -> None:
        self.fail_count += 1
        self.lower_bound = self.current_interval
        self.stable = False
        if self.upper_bound is not None:
            self.current_interval = (self.upper_bound + self.lower_bound) // 2
        else:
            self.current_interval = self.floor
        self.current_interval = self._clamp(self.current_interval)
        if self.fail_count >= 3:
            self.current_interval = self.floor


class Path:
    """单条路径的运行时状态。"""

    __slots__ = ("path_id", "proto", "role", "scheduler", "last_seen", "rtt_ms", "cfg")

    def __init__(self, path_id: str, proto: str, role: str,
                 rtt_ms: int = 0, config=None) -> None:
        self.cfg = config
        self.path_id = path_id
        self.proto = proto
        self.role = role
        self.scheduler = KeepaliveScheduler(role, proto, config=config)
        self.last_seen: float = time.time()
        self.rtt_ms = rtt_ms

    def set_role(self, role: str) -> None:
        """改角色并同步重建 scheduler（保活参数随角色变化）。"""
        self.role = role
        self.scheduler = KeepaliveScheduler(role, self.proto, config=self.cfg)


class PeerPathScheduler:
    """单个 peer 的路径分级与保活决策。

    路径来源：
      · TCP 打洞成功 → 一条 "tcp" 路径
      · UDP 打洞成功 → 一条 "udp" 路径
    按协议优先级（TCP > UDP）+ RTT 分级。
    """

    def __init__(self, peer_id: str,
                 log: Optional[Callable[[str], None]] = None,
                 config=None) -> None:
        if config is None:
            from ..config import DEFAULT_CONFIG
            config = DEFAULT_CONFIG
        self.cfg = config
        self.peer_id = peer_id
        self.log = log or (lambda m: None)
        self.lock = threading.Lock()
        self.paths: Dict[str, Path] = {}      # {path_id: Path}

    def register(self, path_id: str, proto: str, rtt_ms: int) -> Path:
        """新路径接入。已存在且未失效则直接复用，否则新建并重排角色。"""
        with self.lock:
            existing = self.paths.get(path_id)
            if existing is not None and existing.role != C.ROLE_DEAD:
                return existing
            # 新路径先以 warm_loose 占位，再由 regrade 按 (优先级, RTT) 重新分级；
            # 不能用 ROLE_DEAD 占位，否则会被 _regrade_locked 过滤掉、永不上位。
            p = Path(path_id, proto, C.ROLE_WARM_LOOSE, rtt_ms, config=self.cfg)
            self.paths[path_id] = p
            self._regrade_locked()
        self.log("[路径] peer=%s 注册 %s/%s rtt=%sms role=%s"
                 % (self.peer_id, path_id, proto, rtt_ms, p.role))
        return p

    def _regrade_locked(self) -> None:
        """按协议优先级（TCP > UDP）+ RTT 重排存活路径角色。"""
        alive = [p for p in self.paths.values() if p.role != C.ROLE_DEAD]
        alive.sort(key=lambda x: (self.cfg.proto_priority.get(x.proto, 9), x.rtt_ms))
        roles = [C.ROLE_HOT, C.ROLE_WARM_SAFE, C.ROLE_WARM_LOOSE]
        for i, p in enumerate(alive):
            new_role = roles[i] if i < len(roles) else C.ROLE_WARM_LOOSE
            if p.role != new_role:
                p.set_role(new_role)

    def regrade(self) -> None:
        with self.lock:
            self._regrade_locked()

    def remove(self, path_id: str) -> None:
        with self.lock:
            self.paths.pop(path_id, None)

    def get_by_role(self, role: str) -> Optional[Path]:
        with self.lock:
            for p in self.paths.values():
                if p.role == role:
                    return p
        return None

    def get_hot(self) -> Optional[Path]:
        return self.get_by_role(C.ROLE_HOT)

    def promote_on_hot_failure(self) -> Optional[str]:
        """热备失效 → 保守暖备上位，宽松暖备升保守。返回新热备 path_id。"""
        with self.lock:
            hot = safe = loose = None
            for p in self.paths.values():
                if p.role == C.ROLE_HOT:
                    hot = p
                elif p.role == C.ROLE_WARM_SAFE:
                    safe = p
                elif p.role == C.ROLE_WARM_LOOSE:
                    loose = p
            if hot:
                hot.role = C.ROLE_DEAD
            new_hot = None
            if safe:
                safe.set_role(C.ROLE_HOT)
                new_hot = safe.path_id
            if loose:
                loose.set_role(C.ROLE_WARM_SAFE)
        return new_hot

    def role_of(self, path_id: str) -> Optional[str]:
        with self.lock:
            p = self.paths.get(path_id)
            return p.role if p else None
