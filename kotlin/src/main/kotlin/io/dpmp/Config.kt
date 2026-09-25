package io.dpmp

import io.dpmp.protocol.C

/**
 * DPMP 运行时配置（调优参数）。
 *
 * 协议常量见 io.dpmp.protocol.C，**故意不可配**——它们是对端约定。
 * 本类只放**不影响兼容性**的调优参数。
 *
 * 与 Python 端 dpmp/config.py 对齐。
 */
class Config {
    // ---- UDP-RTP ----
    var rtpMaxPayload: Int = C.RTP_MAX_PAYLOAD
    var rtpWindow: Int = C.RTP_WINDOW
    var rtpWindowMin: Int = C.RTP_WINDOW_MIN
    var rtpWindowMax: Int = C.RTP_WINDOW_MAX
    var rtpWindowInit: Int = C.RTP_WINDOW_INIT
    var rtpRtoMs: Long = C.RTP_RTO_MS
    var rtpRtxInterval: Double = C.RTP_RTX_INTERVAL
    var rtpKeepaliveInterval: Double = C.RTP_KEEPALIVE_INTERVAL
    var rtpPeerDeadTimeout: Double = C.RTP_PEER_DEAD_TIMEOUT

    // ---- SYNC（TCP 打洞时刻对齐）----
    var syncSampleCount: Int = C.SYNC_SAMPLE_COUNT
    var syncTimeout: Double = C.SYNC_TIMEOUT
    var syncCommitDelayMs: Long = C.SYNC_COMMIT_DELAY_MS
    var syncCommitResend: Int = C.SYNC_COMMIT_RESEND
    var syncCommitResendInterval: Double = C.SYNC_COMMIT_RESEND_INTERVAL

    // ---- 打洞 ----
    var punchConnectTimeout: Double = C.PUNCH_CONNECT_TIMEOUT
    var punchConcurrency: Int = C.PUNCH_CONCURRENCY
    var punchCandidateConcurrency: Int = C.PUNCH_CANDIDATE_CONCURRENCY
    var punchRetry: Int = C.PUNCH_RETRY
    var punchRetryBackoff: Int = C.PUNCH_RETRY_BACKOFF
    var punchPortPredictRange: IntArray = C.PUNCH_PORT_PREDICT_RANGE.copyOf()

    // ---- 信令 ----
    var heartbeatInterval: Int = C.HEARTBEAT_INTERVAL
    var recvTimeout: Double = C.RECV_TIMEOUT
    var udpProbeInterval: Double = C.UDP_PROBE_INTERVAL
    var udpProbeDuration: Double = C.UDP_PROBE_DURATION

    // ---- 路径调度（梯度冗余多路径）----
    var protoFloor: MutableMap<String, Int> = C.PROTO_FLOOR.toMutableMap()
    var protoSafeCap: MutableMap<String, Int> = C.PROTO_SAFE_CAP.toMutableMap()
    var roleFactor: MutableMap<String, Double> = C.ROLE_FACTOR.toMutableMap()
    var protoPriority: MutableMap<String, Int> = C.PROTO_PRIORITY.toMutableMap()

    // ---- 连接管理 ----
    var rebuildCooldownSec: Double = 30.0
    var keepaliveTick: Double = 1.0
    var idleFactorTiers: List<Pair<Int, Int>> = listOf(60 to 1, 600 to 4, 3600 to 16)
    var idleFactorMax: Int = 64

    // ---- 局域网发现 ----
    var discoverInterval: Int = C.HEARTBEAT_INTERVAL

    companion object {
        /** 全局默认配置（未显式传 config 的实例都用它）。 */
        val DEFAULT: Config = Config()
    }
}
