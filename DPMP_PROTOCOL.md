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
