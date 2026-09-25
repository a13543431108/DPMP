# DPMP · Kotlin 端（Dual-Punch Multi-Path Protocol）

**DPMP 的 Kotlin/JVM 实现** —— 与 Python 端 (` + FENCE + `dpmp/` + FENCE + `) 协议逐字节一致的 P2P 连接与传输底层库。

只做一件事：**把字节可靠地从 A 送到 B**。你提供「服务器 + 房间号」，
它返回「一个可收发 `bytes` 的通道」。

依赖：**纯 JVM 标准库，零第三方**。可被 Android 与桌面 JVM 直接依赖。

---

## 与 Python 端的关系

| 项 | 值 |
|---|---|
| 协议标识 | `DPMP/1.0` |
| 库版本 | 0.1.4 |
| 编解码 | 大端序，与 ```dpmp/protocol/codec.py``` 逐字节一致 |
| 一致性保障 | 共享 ```test_vectors.json```，两端单测各自加载断言 |
| 分层 | protocol / link / stream / util（与 Python 端同名对齐） |

### 分层对照

| Python | Kotlin |
|---|---|
| `dpmp.protocol` | `io.dpmp.protocol`（C / Codec / Addr / Json） |
| `dpmp.link` | `io.dpmp.link`（Discovery / SignalingClient / HolePuncher / UdpReliableSocket / PeerPathScheduler / LinkManager / Natprobe） |
| `dpmp.stream` | `io.dpmp.stream`（DpmpSocket / Channel） |
| `dpmp.util` | `io.dpmp.util`（Net） |
| `dpmp.config.Config` | `io.dpmp.Config` |
| `dpmp.defaults` | `io.dpmp.Defaults` |

---

## 构建

需要 **JDK 17+**（推荐 21）。

```bash
cd kotlin
./gradlew build        # Windows: gradlew.bat build
```

产物：

- `build/libs/dpmp-0.1.4.jar` —— 库 JAR
- `build/libs/dpmp-0.1.4-sources.jar` —— 源码 JAR

---

## 快速开始

### 1. 公网模式（异地互传）

```kotlin
import io.dpmp.link.SignalingClient
import io.dpmp.link.LinkManager

val sig = SignalingClient(
    serverIp = "your.server.com",
    serverPort = 3336,
    room = "my_room",
    password = "",          // 可选：留空=开放房间；填了=受保护房间（须密码一致）
    name = "my_pc",
    tcpPort = 9998,
    punchLocalPort = 9998,
    udpHolePort = 9996,
    deviceId = "dev-001"
)

val lm = LinkManager(sig, localTcpPort = 9998)

// 把信令事件接到连接管理（必须）
sig.onJoined         = { lm.onJoined(it) }
sig.onMemberJoin     = { lm.onMemberJoin(it) }
sig.onMemberLeave    = { lm.onMemberLeave(it) }
sig.onPunchGo        = { peer, at -> lm.onPunchGo(peer, at) }
sig.onUdpHoleReady   = { pid, addr -> lm.onUdpHoleReady(pid, addr) }
sig.onMappingReady   = { lm.markMappingReady() }

// 连接就绪回调 —— 在这里跑你自己的协议
lm.onSocketReady = { peerId, sock, member ->
    sock.sendall("hello".toByteArray())
    val data = sock.recv(4096)
}

lm.start()
sig.start()
```

### 2. 局域网模式（无需服务器）

```kotlin
import io.dpmp.link.Discovery

val d = Discovery(
    deviceId = "dev-001",
    hostname = "my-pc",
    onNewNode = { ip, msg -> println("发现设备 " + ip) }
)
d.start()
d.broadcastSearch()
d.scanSubnet("192.168.1.0/24")
println(d.getNodes())
```

### 3. 主动发送（按路径角色选路）

```kotlin
val ch = lm.getSendChannel(peerId)   // SendChannel(sock, ioLock, isUdp, role) 或 null
if (ch != null) {
    synchronized(ch.ioLock) {
        ch.sock.sendall("data".toByteArray())
    }
}
```

### 4. 单独使用打洞

```kotlin
import io.dpmp.link.HolePuncher

val p = HolePuncher(localTcpPort = 9998)
p.setUdpHint(peerId, ip, udpPort)   // 双打洞：用 UDP 端口预测 TCP 候选
val result = p.punch(peerMap, atMs) // 并行打洞，返回 PunchResult 或 null
```

### 5. 网络切换重建（接口反转）

DPMP 平台无关，**由宿主检测网络变化**，检测到后调用一行：

```kotlin
// Android：ConnectivityManager.NetworkCallback.onAvailable 里
// 桌面：定时轮询本机 IP 集合，发现变化时
lm.onNetworkChanged()
```

带 10 秒冷却。内部自动：关闭旧连接 → 信令重绑（复用身份）→ 重新打洞。

---

## 配置

```kotlin
import io.dpmp.Config

val cfg = Config().apply {
    rtpWindowInit = 64
    rtpWindowMax = 256
    punchRetry = 5
    punchConnectTimeout = 4.0
    heartbeatInterval = 20
}
val sig = SignalingClient(room = "r", name = "n", config = cfg)
val lm  = LinkManager(sig, 9998, config = cfg)
```

**分层原则**：协议常量不可配（改了与对端不兼容）；调优参数全部可配。

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
| 9998 | UDP | 局域网设备发现 |
| 9999 | TCP | 局域网文件传输 |

---

## 测试

```bash
./gradlew test
```

单测加载仓库根目录的 `test_vectors.json`，断言 Kotlin 端编解码与 Python 端逐字节一致。

---

## 许可证

Apache License 2.0（与 Python 端一致）。
