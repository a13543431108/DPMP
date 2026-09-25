# DPMP · Dual-Punch Multi-Path Protocol

**双打洞多路径协议** —— 一个 P2P 连接与传输底层库。

DPMP 只做一件事：**把字节可靠地从 A 送到 B**。
你提供「服务器 + 房间号」，它返回「一个可收发 `bytes` 的通道」；
至于这些字节是文件、视频帧还是游戏消息 —— 由你的上层定义。

适用场景：文件互传、远程控制、投屏、游戏房间等一切需要 P2P 的软件底座。

---

## 核心能力（两项独创）

1. **双打洞协同（Dual-Punch）**
   先通过 UDP 打洞建立可靠控制通道（UDP-RTP），再用该通道作为**控制面**
   协调 TCP 打洞（轮次对齐、候选并行、就绪信号、失败通知）。
   控制面与数据面分离，显著提升 TCP 打洞成功率。

2. **梯度冗余多路径（Multi-Path）**
   同一对端维护多条路径，按 **hot / warm_safe / warm_loose** 三级角色分级调度：
   热备承载数据、保守暖备待命、宽松暖备兜底。热备断线自动接力、换路续传。

---

## 分层架构

| 层 | 模块 | 职责 |
|---|---|---|
| 协议 | `dpmp.protocol` | 线格式常量与编解码（两端唯一事实来源，不可配） |
| 连接 | `dpmp.link` | 发现 / 信令 / 双打洞 / UDP-RTP / 路径调度 / 连接管理 |
| 传输 | `dpmp.stream` | 统一字节流通道（屏蔽 TCP / UDP-RTP 差异） |
| 配置 | `dpmp.config` | 调优参数（全可配） |
| 默认 | `dpmp.defaults` | 默认信令服务器（便利入口，可覆盖） |
| 工具 | `dpmp.util` | 本机 IP / 子网 / 广播地址 / 设备标识 |

依赖：**纯标准库，零第三方**。

---

## 安装

```bash
pip install -e .
```

---

## 快速开始

### 1. 公网模式（异地互传）

```python
from dpmp.link.signaling import SignalingClient
from dpmp.link.manager import LinkManager

# ① 信令客户端（不传服务器则用内置默认；见「默认服务器」一节）
sig = SignalingClient(
    servers=["your.server.com:3336", "backup.com:3336"],  # 多服务器故障转移
    room="my_room",          # 房间号，两端填同一个
    name="my_pc",            # 显示名
    tcp_port=9998,           # 本机打洞 / 长连接端口
    lan_ips=[],              # 本机局域网 IP（可空）
    device_id="dev-001",     # 稳定设备标识（去重用）
    punch_local_port=9998,   # 打洞本地端口
    udp_hole_port=9996,      # UDP 打洞端口
)

# ② 连接管理（打洞 + 多路径 + 保活）
lm = LinkManager(sig, local_tcp_port=9998)

# ③ 把信令事件接到连接管理（必须）
sig.on_joined         = lm.on_joined
sig.on_member_join    = lm.on_member_join
sig.on_member_leave   = lm.on_member_leave
sig.on_punch_go       = lm.on_punch_go
sig.on_udp_hole_ready = lm.on_udp_hole_ready
sig.on_mapping_ready  = lm.mark_mapping_ready

# ④ 连接就绪回调 —— 在这里跑你自己的协议
def on_ready(peer_id, sock, member):
    sock.sendall(b"hello")          # 发
    data = sock.recv(4096)          # 收
lm.on_socket_ready = on_ready

# ⑤ 启动
lm.start()
sig.start()     # 依次尝试 servers，第一个成功即停

# ⑥ 网络切换时重建（宿主检测到后调用，见「网络切换重建」一节）
#    示例：Android 在 NetworkCallback 里、桌面在 IP 轮询里调用
def on_network_changed():
    lm.on_network_changed()   # 关闭旧连接 + 信令重绑 + 重新打洞（复用身份）
```

### 2. 局域网模式（**无需服务器**）

```python
from dpmp.link.discovery import Discovery

d = Discovery(
    device_id="dev-001", hostname="my-pc",
    on_new_node=lambda ip, msg: print("发现设备", ip),
)
d.start()
d.broadcast_search()                # 广播搜索
d.scan_subnet("192.168.1.0/24")     # 扫描子网
print(d.get_nodes())                # {ip: {hostname, device_id, mac, ...}}
```

### 3. 主动发送（按路径角色选路）

```python
ch = lm.get_send_channel(peer_id)      # 返回 (sock, io_lock, is_udp, role) 或 None
if ch:
    sock, lock, is_udp, role = ch      # role: hot / warm_safe / warm_loose
    with lock:
        sock.sendall(b"data")
```

### 4. 单独使用打洞

```python
from dpmp.link.puncher import HolePuncher

p = HolePuncher(local_tcp_port=9998)
p.set_udp_hint(peer_id, ip, udp_port)   # 双打洞：用 UDP 端口预测 TCP 候选
result = p.punch(peer, at_ms)           # 并行打洞，返回 PunchResult（含 sock）
```

### 5. 网络切换重建（接口反转）

DPMP 平台无关，**由宿主检测网络变化**，检测到后调用一行：

```python
# 宿主：检测到 Wi-Fi/蜂窝切换、IP 变化时调用
lm.on_network_changed()
```

DPMP 内部自动完成：关闭旧连接 → 信令重开并重新加入（复用旧 id，无感重建）
→ 对所有成员重新打洞。带 10 秒冷却。

宿主检测示例（平台相关）：
```python
# Android：ConnectivityManager.NetworkCallback.onAvailable 里
# 桌面：定时轮询本机 IP 集合，发现变化时
# 只要最终调用 lm.on_network_changed() 即可
```

---

## 服务器地址

### 三种写法

```python
# 写法一：IP + 端口
SignalingClient(server_ip="1.2.3.4", server_port=3336, ...)

# 写法二：多服务器列表（第一个失败自动切下一个）
SignalingClient(servers=[
    "primary.example.com:3336",
    "backup1.example.com:3336",
    ("backup2.example.com", 3336, 3337, 3338),   # ip, port, tcp_port, nat_port
], auto_failover=True, ...)

# 写法三：字典
SignalingClient(servers=[{"ip": "1.2.3.4", "port": 3336}], ...)
```

### 默认服务器（便利入口，非依赖）

不传任何服务器时，回退到内置默认服务器，方便开箱即用：

```python
from dpmp import DEFAULT_SERVER, DEFAULT_SERVER_EXPIRES, check_default_server_expiry

print(DEFAULT_SERVER)            # "42.194.133.132"
print(DEFAULT_SERVER_EXPIRES)    # "2026-11-01"
print(check_default_server_expiry())
# {'status': 'ok', 'days_left': 38, 'expires': '2026-11-01'}
```

默认服务器**只是便利**，不是依赖：

- 使用默认服务器时，`start()` 会自动检查到期并在临近/过期时提示；
- 传了自己的 `server_ip` / `servers` 后，默认服务器**完全被绕过**；
- **局域网模式根本不使用服务器**。

> ⚠️ 默认服务器到期后，请改用自建服务器或备用服务器。
> 修改默认值见 `dpmp/defaults.py`。

---

## 配置

调优参数通过 `Config` 按实例配置（协议常量不可配）：

```python
from dpmp import Config

cfg = Config(
    rtp_window_init=64,           # UDP-RTP 自适应窗口初始值
    rtp_window_min=1,             #   自适应窗口下限
    rtp_window_max=256,           #   自适应窗口上限（AIMD 动态调节）
    rtp_keepalive_interval=10,    # 保活间隔
    punch_retry=5,                # 打洞重试次数
    punch_connect_timeout=4.0,    # 打洞连接超时
    punch_concurrency=8,          # 打洞并发任务数
    punch_candidate_concurrency=2,# 单次打洞中【并发 connect 的候选数】上限
    heartbeat_interval=20,        # 信令心跳间隔
    proto_floor={"tcp": 30, "udp": 15},   # 保活硬性下限
    rebuild_cooldown_sec=30.0,    # 重建冷却
)
sig = SignalingClient(..., config=cfg)
lm  = LinkManager(sig, 9998, config=cfg)

# 或改全局默认
import dpmp
dpmp.DEFAULT_CONFIG.rtp_window = 200
```

**分层原则**：
- **协议常量不可配**（改了与对端不兼容）
- **调优参数、地址端口全部可配**

---

## 自建服务器

信令服务器**独立开源**（见项目 `服务器/信令服务器.py`），
只牵线、不传数据、带宽近乎为零。可自行部署：

- Linux：`install_linux.sh`（systemd）
- Windows：`install_windows.bat`（计划任务）
- 配置：`config.json`（端口、房间上限、心跳超时等）

服务器地址通过 `server_ip` / `servers` 传给 `SignalingClient`，
客户端**不写死任何服务器**。

---

## API 参考

### 顶层（`import dpmp`）

| 名称 | 说明 |
|---|---|
| `Config` / `DEFAULT_CONFIG` | 调优参数 |
| `DEFAULT_SERVER` / `DEFAULT_SERVER_EXPIRES` | 默认服务器与到期日 |
| `default_servers()` | 默认服务器候选列表 |
| `check_default_server_expiry()` | 检查默认服务器到期状态 |
| `__version__` / `__protocol__` | 版本 / 协议标识 |

### 连接层（`dpmp.link`）

| 名称 | 说明 |
|---|---|
| `Discovery` | 局域网 UDP 发现（广播 / 扫描 / 心跳） |
| `SignalingClient` | 公网信令客户端（房间 / 校时 / 映射观测 / NAT 探测） |
| `HolePuncher` / `PunchResult` | 双打洞核心 / 打洞结果 |
| `UdpReliableSocket` | UDP 可靠通道（UDP-RTP） |
| `PeerPathScheduler` / `Path` / `KeepaliveScheduler` | 梯度冗余多路径调度 |
| `LinkManager` / `Conn` | 连接管理 / 单条连接 |

### 传输层（`dpmp.stream`）

| 名称 | 说明 |
|---|---|
| `Channel` | 统一字节流通道（`sendall` / `recv` / `recv_exact` / `close`） |
| `wrap_channel(sock, io_lock, role)` | 包装底层 socket |
| `is_udp_rtp(sock)` | 判断是否 UDP-RTP 通道 |

### 工具层（`dpmp.util.net`）

| 名称 | 说明 |
|---|---|
| `get_all_local_ips(ipv6=False)` | 本机所有 IP |
| `get_all_subnets()` | 本机所有子网 CIDR |
| `get_broadcast_addrs()` | 广播地址 |
| `get_subnet_for_ip(ip)` | 由 IP 推断子网 |
| `get_mac_address()` | 本机 MAC |
| `get_or_create_device_id(path)` | 持久化设备 UUID |

---

## 关键回调与方法

### LinkManager 回调

| 回调 | 触发时机 |
|---|---|
| `on_socket_ready(peer_id, sock, member)` | TCP 通道建立（**最常用**） |
| `on_udp_ready(peer_id, rtp)` | UDP-RTP 通道建立 |
| `on_state_changed()` | 成员状态变化（刷新 UI） |

### LinkManager 方法

| 方法 | 用途 |
|---|---|
| `get_send_channel(peer_id, exclude_socks=None)` | 按角色选路，返回可发送通道 |
| `get_socket(peer_id)` | 取 TCP socket |
| `get_udp_socket(peer_id)` | 取 UDP-RTP 通道 |
| `get_members()` | 成员列表（含状态、路径角色） |
| `get_path_roles(peer_id)` | 各路径角色 |
| `mark_active(peer_id)` | 标记活跃（重置空闲降频） |
| `on_network_changed()` | **网络切换时由宿主调用**，重建所有连接（见下节） |
| `start()` / `stop()` | 启动 / 停止 |

---

## 网络切换重建（接口反转）

网络切换（Wi-Fi ↔ 蜂窝、IP 变化）后，旧网络上的所有 socket 与 NAT 映射全部失效。
DPMP 是**平台无关**的库，无法自己感知网络变化，因此采用「接口反转」：

- **宿主应用负责【检测】**（平台相关：Android NetworkCallback、桌面 IP 轮询等）
- **DPMP 负责【重建】**（统一逻辑，所有用户受益）

宿主检测到网络变化后，只需一行调用：

```python
lm.on_network_changed()
```

DPMP 内部编排完整重建：

1. 关闭所有现存连接（旧 NAT 映射已失效）
2. 让信令客户端**重开 socket 并重新加入**（`SignalingClient.rebind()`）
3. 对所有成员**重新发起打洞**

**无感重建**：重建时
- 不发 BYE（服务器保留成员条目，对端不掉线）
- 携带旧 `my_id`（`reuse_id`）——服务器支持则**复用身份**，对端看到的是
  「同一成员回归」；旧服务器忽略该字段，退化为生成新 id（等价重新 join）

带 10 秒冷却，避免网络抖动频繁重建。

---

## 端口说明

| 端口 | 协议 | 用途 |
|---|---|---|
| 3336 | UDP | 信令服务器（加入房间 / 心跳 / 打洞协调） |
| 3337 | TCP | 信令服务器 TCP 映射观测 |
| 3338 | UDP | 信令服务器 NAT 类型探测 |
| 9996 | UDP | 房间模式 UDP 打洞 |
| 9998 | TCP | 房间模式打洞 / 长连接 |
| 9997 | UDP | 局域网主动扫描 |
| 9998 | UDP | 局域网设备发现（广播 / 心跳） |
| 9999 | TCP | 局域网文件传输 |

所有端口均可通过参数或 `Config` 覆盖。

---

## 协议文档

- `DPMP_PROTOCOL.md` —— 完整协议规范（两端实现唯一事实来源）
- `test_vectors.json` —— 协议测试向量（防 Python / Kotlin 两端漂移）

---

## 版本历史

### 0.1.1（底层能力补强）

- **新增**：UDP-RTP 自适应窗口（AIMD）——发送端按 ACK 加性增、按超时乘性减，
  在 1~256 之间动态调节；高延迟链路上吞吐上限提升数倍。
- **新增**：打洞候选并发限制（`punch_candidate_concurrency`，默认 2）——
  降低同一本地端口上 SO_REUSEPORT 的入站 SYN 匹配冲突。
- **新增**：网络切换重建（接口反转）——`SignalingClient.rebind()` +
  `LinkManager.on_network_changed()`；宿主检测网络变化，DPMP 统一重建。
- **新增**：身份复用（`reuse_id`）——重建时复用旧 my_id，实现无感重建。
- **修复**：收到对端 PUNCH_FAIL 时未关闭半开连接——该 socket 可能被
  `get_send_channel` 选为发送通道，把数据发进单向死连接。现在无条件关闭并
  触发路径轮转。

### 0.1.0（初版）

- 双打洞协同、UDP-RTP 可靠通道、梯度冗余多路径、局域网发现、公网信令。

---

## 许可证

本项目采用 **Apache License 2.0** 开源许可，详见 `LICENSE` 文件。

你可以自由使用、修改、分发本项目（含商业用途），需保留版权声明与许可声明。
