# -*- coding: utf-8 -*-
"""DPMP 运行时配置（调优参数）。

协议常量（消息类型 / 包类型 / 字段格式）见 dpmp.protocol.constants，
**故意不可配**——它们是对端约定，改了就不兼容。

本模块只放**不影响兼容性**的调优参数，支持按实例覆盖：

    from dpmp import Config
    cfg = Config()
    cfg.rtp_window = 128
    sig = SignalingClient(..., config=cfg)

全局默认：dpmp.config.DEFAULT_CONFIG（改它影响所有未显式传 config 的实例）。
"""

from typing import Any, Dict, List, Tuple

from .protocol import constants as C


class Config:
    """DPMP 调优参数集合。所有字段都有默认值，可逐个覆盖。"""

    def __init__(self, **kw: Any) -> None:
        self.reset()
        for k, v in kw.items():
            if hasattr(self, k):
                setattr(self, k, v)

    def reset(self) -> None:
        """恢复全部默认值。"""
        # ---- UDP-RTP ----
        self.rtp_max_payload = C.RTP_MAX_PAYLOAD
        self.rtp_window = C.RTP_WINDOW          # 初始值（兼容名）
        self.rtp_window_min = C.RTP_WINDOW_MIN  # 自适应窗口下限
        self.rtp_window_max = C.RTP_WINDOW_MAX  # 自适应窗口上限
        self.rtp_window_init = C.RTP_WINDOW_INIT
        self.rtp_rto_ms = C.RTP_RTO_MS
        self.rtp_rtx_interval = C.RTP_RTX_INTERVAL
        self.rtp_keepalive_interval = C.RTP_KEEPALIVE_INTERVAL
        self.rtp_peer_dead_timeout = C.RTP_PEER_DEAD_TIMEOUT
        # ---- SYNC（TCP 打洞时刻对齐）----
        self.sync_sample_count = C.SYNC_SAMPLE_COUNT
        self.sync_timeout = C.SYNC_TIMEOUT
        self.sync_commit_delay_ms = C.SYNC_COMMIT_DELAY_MS
        self.sync_commit_resend = C.SYNC_COMMIT_RESEND
        self.sync_commit_resend_interval = C.SYNC_COMMIT_RESEND_INTERVAL
        # ---- 打洞 ----
        self.punch_connect_timeout = C.PUNCH_CONNECT_TIMEOUT
        self.punch_concurrency = C.PUNCH_CONCURRENCY
        self.punch_candidate_concurrency = C.PUNCH_CANDIDATE_CONCURRENCY
        self.punch_retry = C.PUNCH_RETRY
        self.punch_retry_backoff = C.PUNCH_RETRY_BACKOFF
        self.punch_port_predict_range = tuple(C.PUNCH_PORT_PREDICT_RANGE)
        # ---- 信令 ----
        self.heartbeat_interval = C.HEARTBEAT_INTERVAL
        self.recv_timeout = C.RECV_TIMEOUT
        self.udp_probe_interval = C.UDP_PROBE_INTERVAL
        self.udp_probe_duration = C.UDP_PROBE_DURATION
        # ---- 路径调度（梯度冗余多路径）----
        self.proto_floor = dict(C.PROTO_FLOOR)
        self.proto_safe_cap = dict(C.PROTO_SAFE_CAP)
        self.role_factor = dict(C.ROLE_FACTOR)
        self.proto_priority = dict(C.PROTO_PRIORITY)
        # ---- 连接管理 ----
        self.rebuild_cooldown_sec = 30.0
        self.keepalive_tick = 1.0
        self.idle_factor_tiers = [(60, 1), (600, 4), (3600, 16)]
        self.idle_factor_max = 64
        # ---- 局域网发现 ----
        self.discover_interval = C.HEARTBEAT_INTERVAL


# 全局默认配置（未显式传 config 的实例都用它）
DEFAULT_CONFIG = Config()
