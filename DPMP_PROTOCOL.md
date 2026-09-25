# DPMP 协议规范 · Dual-Punch Multi-Path Protocol

版本：DPMP/1.0
本文件是两端（Python / Kotlin）实现的**唯一事实来源**。任何字段、端口、
报文类型的改动，必须同步更新：本文件 + `dpmp/protocol/constants.py` +
`test_vectors.json`。

---

## 1. 分层

| 层 | 职责 | 关键模块 |
|---|---|---|
| protocol | 常量、编解码、地址工具 | `constants` `codec` `addr` |
| link | 发现 / 信令 / 双打洞 / UDP-RTP / 路径调度 / 连接管理 | `discovery` `signaling` `puncher` `rtp` `path` `manager` |
| stream | 统一字节流通道 + 多径选路 | `channel` |

---

## 2. 端口

| 端口 | 协议 | 用途 |
|---|---|---|
| 9997 | UDP | 局域网主动扫描 |
| 9998 | UDP | 局域网设备发现（广播 / 心跳） |
| 9999 | TCP | 局域网文件传输 |
| 3336 | UDP | 信令服务器（加入房间 / 心跳 / 打洞协调） |
| 3337 | TCP | 信令服务器 TCP 映射观测 |
| 3338 | UDP | 信令服务器 NAT 类型探测 |
| 9996 | UDP | 房间模式 UDP 打洞 |
| 9998 | TCP | 房间模式打洞 / 长连接 |

---

## 3. 信令消息（JSON over UDP）

字段 `type` 区分。版本字段 `ver`。

### 客户端 → 服务器
| type | 说明 |
|---|---|
| `join` | 加入房间 |
| `hb` | 心跳 |
| `punch_req` | 请求协调打洞 |
| `bye` | 离开 |
| `nat_probe` | 请求回复观测到的公网地址 |
| `time_req` | NTP 式时间同步请求 |
| `udp_hello` | 登记 UDP 打洞 socket 公网映射 |

### 服务器 → 客户端
| type | 说明 |
|---|---|
| `joined` | 加入成功，附成员列表 |
| `nat_probe_reply` | 观测到的公网地址 |
| `member_join` / `member_leave` | 成员上下线 |
| `punch_go` | 通知在 at_ms 时刻打洞 |
| `time_reply` | 回显 t1 + 服务器时刻 t2 |
| `error` | 错误 |

---

## 4. UDP-RTP（可靠 UDP 通道）

### 包头（12 字节，大端）
```
[1B type][4B seq][4B ack][2B length][1B reserved]
```

### 包类型
| 值 | 名称 | 说明 |
|---|---|---|
| 0 | DATA | 数据 |
| 1 | ACK | 确认 |
| 2 | FIN | 关闭 |
| 3 | KEEPALIVE | 空闲保活，刷新 NAT conntrack |
| 4 | SYNC_REQ | 发起方 SYNC 请求 |
| 5 | SYNC_ACK | 响应方 SYNC 响应 |
| 6 | SYNC_COMMIT | 发起方约定 TCP 打洞时刻 |
| 7 | PUNCH_ROUND | 打洞轮次对齐（主导方约定下一轮时刻） |
| 8 | TCP_READY | TCP 映射已就绪 |
| 9 | PUNCH_FAIL | 打洞彻底失败通知 |

### 控制报文 payload
| 报文 | 格式 | 字段 |
|---|---|---|
| SYNC_REQ | `!QB` | t1(ms), proto_ver |
| SYNC_ACK | `!QQQB` | t1, t2, t3, proto_ver |
| SYNC_COMMIT v2 | `!QQ` | t_go_r(ms), commit_id |
| SYNC_COMMIT v1 | `!Q` | delta_ms |
| PUNCH_ROUND | `!QQ` | round_no, t_go_r |

---

## 5. 双打洞协同（Dual-Punch）

1. 两端各自 UDP 打洞 → 建立 UDP-RTP 可靠通道；
2. 发起方在 UDP-RTP 上 `start_sync()`：采样 RTT，发 SYNC_COMMIT 约定 T_go；
3. 两端到点并行 connect 所有 TCP 候选（lan / pub_tcp / UDP 端口 ±2 预测）；
4. TCP 映射就绪方广播 TCP_READY，对端立即发起打洞；
5. 谁先连接成功用谁；失败方发 PUNCH_FAIL，对端回退 UDP。

---

## 6. 梯度冗余多路径（Multi-Path）

- 角色：`hot`（传数据）/ `warm_safe`（保守暖备）/ `warm_loose`（宽松暖备）
- 分级：按 (协议优先级 TCP>UDP, RTT) 排序分配角色
- 保活：软性探测（成功指数增长逼近 NAT 超时）+ 硬性下限（TCP 30s / UDP 15s）
- 保守暖备硬性钳制：不超过协议安全上限（TCP 3600s / UDP 60s）
- 热备断裂 → 保守暖备上位 → 宽松暖备升级 → 重建补足 → 跨路径重传

---

## 7. 编解码精确布局（两端实现必须逐字节一致）

本节是 Python / Kotlin 两端二进制编解码的**权威定义**。所有多字节整数均为**大端序（Big-Endian）**，与 struct 的 `!` 前缀一致。

### 7.1 UDP-RTP 包头（12 字节）

```
偏移  长度  字段      类型        说明
 0     1    type      uint8       包类型（见 §4 表）
 1     4    seq       uint32      发送序号（本端自增，从 0 起）
 5     4    ack       uint32      累积确认号（已收到的最大连续序号 + 1）
 9     2    length    uint16      payload 字节数（不含包头）
11     1    reserved  uint8       保留，恒为 0
```

- 结构串：`!BIIHB`（= 12 字节）
- 一个完整报文 = 12 字节包头 + `length` 字节 payload

**测试向量**（`test_vectors.json → rtp_header.sample`）：
```
type=4, seq=10, ack=20, length=9, reserved=0
→ hex: 04 0000000a 00000014 0009 00
```
（type=4 即 SYNC_REQ）

### 7.2 控制报文 payload

| 报文 | 结构串 | 总字节 | 字段布局 |
|---|---|---|---|
| SYNC_REQ | `!QB` | 9 | t1:uint64(0-7) + proto_ver:uint8(8) |
| SYNC_ACK | `!QQQB` | 25 | t1(0-7)+t2(8-15)+t3(16-23)+proto_ver(24) |
| SYNC_COMMIT v2 | `!QQ` | 16 | t_go_r:uint64(0-7) + commit_id:uint64(8-15) |
| SYNC_COMMIT v1 | `!Q` | 8 | delta_ms:uint64(0-7) |
| PUNCH_ROUND | `!QQ` | 16 | round_no:uint64(0-7) + t_go_r:uint64(8-15) |
| TCP_READY | (空) | 0 | 无 payload |
| PUNCH_FAIL | (空) | 0 | 无 payload |
| KEEPALIVE | (空) | 0 | 无 payload |
| FIN | (空) | 0 | 无 payload |
| ACK | (空) | 0 | 无 payload |
| DATA | (任意) | N | 原始字节（≤ RTP_MAX_PAYLOAD=1200） |

**字段含义**：
- t1/t2/t3：NTP 式采样的时间戳（毫秒）
- proto_ver：发送方的 RTP 协议版本（当前 = 2）
- t_go_r：约定的 TCP 打洞时刻（**responder 时钟**的绝对毫秒）
- commit_id：COMMIT 自增轮次号（去重：收到相同 id 直接忽略）
- delta_ms：v1 的相对延迟（从收到 COMMIT 起算）
- round_no：打洞轮次序号

### 7.3 版本协商流程（v1 ↔ v2）

**目的**：新老端互通。v2 引入「绝对时刻 + commit_id」，v1 只有「相对延迟」。

```
① 双方各自在 SYNC_REQ/SYNC_ACK 里带上自己的 proto_ver
② 学习：收到对端报文时，记录 max(本端已知, 报文里的 ver)
③ 发 SYNC_COMMIT 时按【对端版本】选格式：
     · 对端 ver >= 2  → 用 v2（!QQ: t_go_r + commit_id），重传 3 次
     · 对端 ver <  2  → 用 v1（!Q: delta_ms），单发
④ 收 SYNC_COMMIT 时按【payload 长度】自动判别：
     · 16 字节 → v2
     · 8 字节  → v1
```

**版本学习点**（两端对称）：
| 报文 | ver 字段位置 | 作用 |
|---|---|---|
| SYNC_REQ | payload[8] | initiator → responder 通告 |
| SYNC_ACK | payload[24] | responder → initiator 通告 |

### 7.4 test_vectors.json 解读

`test_vectors.json` 是**两端单测共享的断言数据**，防止 Python / Kotlin 实现漂移。两端各自加载它，断言编解码结果一致。

```jsonc
{
  "rtp_header": {
    "struct": "!BIIHB",
    "sample": {
      "type": 4, "seq": 10, "ack": 20, "length": 9, "reserved": 0,
      "hex": "040000000a00000014000900"   // pack 后应等于该 hex
    }
  },
  "sync_req": {
    "struct": "!QB",
    "fields": ["t1_ms", "proto_ver"],
    "sample": { "t1_ms": 123456, "proto_ver": 2 }
    // 断言：pack_sync_req(123456, 2) 往返解出相同值
  }
}
```

**使用方式**（Python 端）：
```python
import json
from dpmp.protocol import codec

tv = json.load(open("test_vectors.json"))

# ① 包头往返
s = tv["rtp_header"]["sample"]
pkt = codec.pack_header(s["type"], s["seq"], s["ack"], s["length"], s["reserved"])
assert pkt.hex() == s["hex"]
assert codec.unpack_header(pkt) == (s["type"], s["seq"], s["ack"], s["length"], s["reserved"])

# ② 控制报文往返
sr = tv["sync_req"]["sample"]
payload = codec.pack_sync_req(sr["t1_ms"], sr["proto_ver"])
assert codec.unpack_sync_req(payload) == (sr["t1_ms"], sr["proto_ver"])
```

**Kotlin 端**应加载同一文件，用 ByteBuffer（BIG_ENDIAN）做相同断言。
