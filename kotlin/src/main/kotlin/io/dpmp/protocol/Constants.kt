package io.dpmp.protocol

/**
 * DPMP 协议常量。
 *
 * 分成三组：
 *   · 信令（房间模式）—— JSON over UDP
 *   · UDP-RTP          —— 12 字节二进制包头
 *   · 打洞/路径调度     —— 时序与分级参数
 *
 * 与 Python 端 dpmp/protocol/constants.py 逐项对齐。
 */
object C {

    // ============================================================
    // 协议版本
    // ============================================================
    const val DPMP_VER = 2          // 信令协议版本（2 = 支持房间密码）
    const val RTP_PROTO_VER = 2     // UDP-RTP 协议版本

    // ============================================================
    // 信令消息类型（JSON over UDP，字段 type）
    // ============================================================
    // 客户端 -> 服务器
    const val T_JOIN = "join"
    const val T_HB = "hb"
    const val T_PUNCH_REQ = "punch_req"
    const val T_BYE = "bye"
    const val T_NAT_PROBE = "nat_probe"
    const val T_TIME_REQ = "time_req"
    const val T_TIME_REPLY = "time_reply"
    const val T_UDP_HELLO = "udp_hello"
    // 服务器 -> 客户端
    const val T_JOINED = "joined"
    const val T_NAT_PROBE_REPLY = "nat_probe_reply"
    const val T_MEMBER_JOIN = "member_join"
    const val T_MEMBER_LEAVE = "member_leave"
    const val T_MEMBER_UPDATE = "member_update"   // 成员信息更新（如 TCP 映射登记完成）
    const val T_PUNCH_GO = "punch_go"
    const val T_ERROR = "error"

    // ============================================================
    // 默认端口
    // ============================================================
    const val DEFAULT_SERVER_PORT = 3336      // 信令服务器 UDP
    const val DEFAULT_SERVER_TCP_PORT = 3337  // TCP 映射观测
    const val NAT_PROBE_PORT = 3338           // NAT 类型探测
    const val ROOM_TCP_PORT = 9998            // 房间模式打洞/长连接
    const val ROOM_UDP_PORT = 9996            // 房间模式 UDP 打洞
    const val LAN_UDP_PORT = 9998             // 局域网发现
    const val LAN_TCP_PORT = 9999             // 局域网文件传输
    const val SCAN_PORT = 9997                // 主动扫描

    // ============================================================
    // UDP-RTP 包头（大端）
    // [1B type][4B seq][4B ack][2B length][1B reserved] = 12 字节
    // ============================================================
    const val RTP_HEADER_SIZE = 12
    const val RTP_MAX_PAYLOAD = 1200
    const val RTP_WINDOW = 64
    const val RTP_WINDOW_MIN = 1
    const val RTP_WINDOW_MAX = 256
    const val RTP_WINDOW_INIT = 64
    const val RTP_RTO_MS = 300L
    const val RTP_RTX_INTERVAL = 0.05
    const val RTP_KEEPALIVE_INTERVAL = 15.0
    const val RTP_PEER_DEAD_TIMEOUT = 90.0

    // UDP-RTP 包类型
    const val RTP_DATA = 0
    const val RTP_ACK = 1
    const val RTP_FIN = 2
    const val RTP_KEEPALIVE = 3
    const val RTP_SYNC_REQ = 4
    const val RTP_SYNC_ACK = 5
    const val RTP_SYNC_COMMIT = 6
    const val RTP_PUNCH_ROUND = 7
    const val RTP_TCP_READY = 8
    const val RTP_PUNCH_FAIL = 9

    // UDP 打洞探测魔数
    val UDP_HOLE_MAGIC: ByteArray = "P2P_UDP_HOLE".toByteArray(Charsets.US_ASCII)

    // ============================================================
    // 局域网发现（JSON over UDP）
    // ============================================================
    const val LAN_K_DISCOVERY = "discovery"
    const val LAN_K_REPLY = "reply"
    const val LAN_K_SCAN_REPLY = "scan_reply"
    const val LAN_K_HEARTBEAT = "heartbeat"
    const val LAN_K_ACK = "ack"
    const val LAN_K_BYE = "bye"
    const val LAN_F_HOSTNAME = "hostname"
    const val LAN_F_DEVICE_ID = "device_id"
    const val LAN_F_MAC = "mac"

    // SYNC 采样
    const val SYNC_SAMPLE_COUNT = 4
    const val SYNC_TIMEOUT = 1.0
    const val SYNC_COMMIT_DELAY_MS = 500L
    const val SYNC_COMMIT_RESEND = 3
    const val SYNC_COMMIT_RESEND_INTERVAL = 0.1

    // ============================================================
    // 打洞参数
    // ============================================================
    const val PUNCH_CONCURRENCY = 6
    const val PUNCH_CANDIDATE_CONCURRENCY = 2
    const val PUNCH_CONNECT_TIMEOUT = 6.0
    const val PUNCH_RETRY = 3
    const val PUNCH_RETRY_BACKOFF = 1
    val PUNCH_PORT_PREDICT_RANGE = intArrayOf(0, -1, 1, -2, 2)

    // ============================================================
    // 路径分级（梯度冗余多路径）
    // ============================================================
    const val ROLE_HOT = "hot"
    const val ROLE_WARM_SAFE = "warm_safe"
    const val ROLE_WARM_LOOSE = "warm_loose"
    const val ROLE_DEAD = "dead"

    val PROTO_FLOOR = mapOf("tcp" to 30, "udp" to 15)
    val PROTO_SAFE_CAP = mapOf("tcp" to 3600, "udp" to 60)
    val ROLE_FACTOR = mapOf("hot" to 1.0, "warm_safe" to 1.5, "warm_loose" to 4.0)
    val PROTO_PRIORITY = mapOf("tcp" to 0, "udp" to 1)

    // 连接状态
    const val STATE_CONNECTING = "connecting"
    const val STATE_CONNECTED = "connected"
    const val STATE_FAILED = "failed"

    // 心跳/超时
    const val HEARTBEAT_INTERVAL = 20
    const val RECV_TIMEOUT = 1.0
    const val NAT_KEEPALIVE_INTERVAL = 15
    const val UDP_PROBE_INTERVAL = 0.1
    const val UDP_PROBE_DURATION = 5.0
}
