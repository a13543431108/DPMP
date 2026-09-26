# -*- coding: utf-8 -*-
"""DPMP 协议常量。

分成三组：
  · 信令（房间模式）—— JSON over UDP
  · UDP-RTP          —— 12 字节二进制包头
  · 打洞/路径调度     —— 时序与分级参数
"""

# ============================================================
# 协议版本
# ============================================================
DPMP_VER = 2                 # 信令协议版本（2 = 支持房间密码）
RTP_PROTO_VER = 2            # UDP-RTP 协议版本（>=2 启用 COMMIT 绝对时刻 + commit_id 去重）

# ============================================================
# 信令消息类型（JSON over UDP，字段 type）
# ============================================================
# 客户端 -> 服务器
T_JOIN = "join"
T_HB = "hb"
T_PUNCH_REQ = "punch_req"
T_BYE = "bye"
T_NAT_PROBE = "nat_probe"
T_TIME_REQ = "time_req"
T_TIME_REPLY = "time_reply"
T_UDP_HELLO = "udp_hello"
# 服务器 -> 客户端
T_JOINED = "joined"
T_NAT_PROBE_REPLY = "nat_probe_reply"
T_MEMBER_JOIN = "member_join"
T_MEMBER_LEAVE = "member_leave"
T_MEMBER_UPDATE = "member_update"   # 成员信息更新（如 TCP 映射登记完成）
T_PUNCH_GO = "punch_go"
T_ERROR = "error"

# ============================================================
# 默认端口
# ============================================================
DEFAULT_SERVER_PORT = 3336          # 信令服务器 UDP
DEFAULT_SERVER_TCP_PORT = 3337      # TCP 映射观测
NAT_PROBE_PORT = 3338               # NAT 类型探测
ROOM_TCP_PORT = 9998                # 房间模式打洞/长连接（与局域网 9999 分离）
ROOM_UDP_PORT = 9996                # 房间模式 UDP 打洞（与局域网发现 9998 分离）
LAN_UDP_PORT = 9998                 # 局域网发现
LAN_TCP_PORT = 9999                 # 局域网文件传输
SCAN_PORT = 9997                    # 主动扫描

# ============================================================
# UDP-RTP 包头（大端）
# [1B type][4B seq][4B ack][2B length][1B reserved] = 12 字节
# ============================================================
RTP_HEADER_SIZE = 12
RTP_MAX_PAYLOAD = 1200
RTP_WINDOW = 64                       # 自适应窗口初始值（兼容名）
# 自适应窗口（AIMD）：发送端按 ACK 加性增、按超时乘性减，动态调在途包数。
#   收到有效 ACK → 窗口 +推进数（封顶 RTP_WINDOW_MAX）
#   超时重传   → 窗口减半（下限 RTP_WINDOW_MIN）
# 接收端乱序接受范围固定 RTP_WINDOW_MAX，保证发送端怎么调都能被接住。
RTP_WINDOW_MIN = 1
RTP_WINDOW_MAX = 256
RTP_WINDOW_INIT = 64
RTP_RTO_MS = 300
RTP_RTX_INTERVAL = 0.05
RTP_KEEPALIVE_INTERVAL = 15.0        # 空闲超过该秒数 → 发 KEEPALIVE 刷新 NAT
RTP_PEER_DEAD_TIMEOUT = 90.0         # 超过该秒数没收到对端任何包 → 判失联

# UDP-RTP 包类型
RTP_DATA = 0
RTP_ACK = 1
RTP_FIN = 2
RTP_KEEPALIVE = 3
RTP_SYNC_REQ = 4        # 发起方发 SYNC 请求
RTP_SYNC_ACK = 5        # 响应方回 SYNC 响应
RTP_SYNC_COMMIT = 6     # 发起方约定 TCP 打洞时刻
RTP_PUNCH_ROUND = 7     # 打洞轮次对齐（主导方约定下一轮时刻）
RTP_TCP_READY = 8       # TCP 映射已就绪信号
RTP_PUNCH_FAIL = 9      # 打洞彻底失败通知

# UDP 打洞探测魔数（两端必须一致）
UDP_HOLE_MAGIC = b"P2P_UDP_HOLE"

# ============================================================
# 局域网发现（JSON over UDP，字段/关键字，两端必须一致）
# ============================================================
# 出站消息关键字（布尔标志）
LAN_K_DISCOVERY = "discovery"     # 广播搜索请求
LAN_K_REPLY = "reply"             # 普通回复
LAN_K_SCAN_REPLY = "scan_reply"   # 扫描回复
LAN_K_HEARTBEAT = "heartbeat"     # 心跳
LAN_K_ACK = "ack"                 # 心跳确认
LAN_K_BYE = "bye"                 # 正常下线
# 消息字段名
LAN_F_HOSTNAME = "hostname"
LAN_F_DEVICE_ID = "device_id"
LAN_F_MAC = "mac"

# SYNC 采样
SYNC_SAMPLE_COUNT = 4           # SYNC RTT 采样次数
SYNC_TIMEOUT = 1.0              # 单次 SYNC 采样超时（秒）
SYNC_COMMIT_DELAY_MS = 500      # 距 SYNC_COMMIT 后多少毫秒执行 TCP 打洞
SYNC_COMMIT_RESEND = 3          # COMMIT 重传次数（抗丢包）
SYNC_COMMIT_RESEND_INTERVAL = 0.1   # COMMIT 重传间隔（秒）

# ============================================================
# 打洞参数
# ============================================================
PUNCH_CONCURRENCY = 6           # 同时打洞的最大任务数
# 单次打洞中【并发 connect 的候选数】上限。所有候选 socket 都绑同一本地
# 端口，并存的越多，SO_REUSEPORT 的入站 SYN 匹配越易分错，成功率越低。
# 限制为 2，在保留少量并行（谁先通用谁）的同时降低端口竞争。
PUNCH_CANDIDATE_CONCURRENCY = 2
PUNCH_CONNECT_TIMEOUT = 6.0     # 单次 TCP connect 超时（秒）
PUNCH_RETRY = 3                 # 打洞失败重试次数
PUNCH_RETRY_BACKOFF = 1         # 重试退避基数（秒）
PUNCH_PORT_PREDICT_RANGE = (0, -1, 1, -2, 2)   # UDP 端口 ±2 预测 TCP 候选

# ============================================================
# 路径分级（梯度冗余多路径）
# ============================================================
ROLE_HOT = "hot"
ROLE_WARM_SAFE = "warm_safe"
ROLE_WARM_LOOSE = "warm_loose"
ROLE_DEAD = "dead"

PROTO_FLOOR = {"tcp": 30, "udp": 15}         # 保活硬性下限（秒）
PROTO_SAFE_CAP = {"tcp": 3600, "udp": 60}    # 保守暖备硬性上限（秒）
ROLE_FACTOR = {"hot": 1.0, "warm_safe": 1.5, "warm_loose": 4.0}
PROTO_PRIORITY = {"tcp": 0, "udp": 1}        # 数字越小越优先当 hot

# 连接状态
STATE_CONNECTING = "connecting"
STATE_CONNECTED = "connected"
STATE_FAILED = "failed"

# 心跳/超时
HEARTBEAT_INTERVAL = 20         # 信令心跳间隔（秒）
RECV_TIMEOUT = 1.0              # 信令 socket 接收超时（秒）
NAT_KEEPALIVE_INTERVAL = 15     # 维持 NAT 映射（秒）
UDP_PROBE_INTERVAL = 0.1
UDP_PROBE_DURATION = 5.0
