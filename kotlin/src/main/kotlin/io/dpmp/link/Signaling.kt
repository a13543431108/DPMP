package io.dpmp.link

import io.dpmp.Config
import io.dpmp.Defaults
import io.dpmp.ServerSpec
import io.dpmp.protocol.Addr
import io.dpmp.protocol.C
import io.dpmp.protocol.Json
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/**
 * UDP 信令客户端：加入房间、心跳、接收成员/打洞事件。
 *
 * 与 Python 端 dpmp/link/signaling.py 对齐。
 *
 * 回调：
 *   onJoined(members)
 *   onMemberJoin(member) / onMemberLeave(peerId)
 *   onPunchGo(peer, atMs)
 *   onError(code)
 *   onUdpHoleReady(peerId, peerAddr)   UDP 打洞成功
 *   onMappingReady()                    TCP 映射就绪
 */
class SignalingClient(
    serverIp: String? = null,
    serverPort: Int? = null,
    val room: String? = null,
    val name: String? = null,
    val tcpPort: Int? = null,
    lanIps: List<String>? = null,
    var onJoined: ((List<Map<String, Any?>>) -> Unit)? = null,
    var onMemberJoin: ((Map<String, Any?>) -> Unit)? = null,
    var onMemberLeave: ((String) -> Unit)? = null,
    var onPunchGo: ((Map<String, Any?>, Long) -> Unit)? = null,
    var onError: ((String) -> Unit)? = null,
    private val log: (String) -> Unit = {},
    serverTcpPort: Int? = null,
    val punchLocalPort: Int? = null,
    val deviceId: String = "",
    var onUdpHoleReady: ((String, Pair<String, Int>) -> Unit)? = null,
    var onMappingReady: (() -> Unit)? = null,
    udpHolePort: Int? = null,
    natProbePort: Int? = null,
    private val config: Config = Config.DEFAULT,
    servers: List<Any>? = null,
    private val autoFailover: Boolean = true,
    private val connectTimeout: Double = 10.0,
    /** 房间密码（可选）：空 = 开放房间。仅存内存，随 join 发送，重建时重发。 */
    private val password: String = ""
) {
    private val serverList: List<ServerSpec>
    private val usingDefaultServer: Boolean

    private var serverIdx = 0
    var serverIp: String = ""
        private set
    var serverPort: Int = 0
        private set
    var serverTcpPort: Int = 0
        private set
    var natProbePort: Int = 0
        private set

    private val roomStr = room ?: ""
    private val lanIpList = lanIps ?: emptyList()
    val udpHolePort: Int = udpPortOrDefault(udpHolePort)

    @Volatile var clockOffsetMs: Long = 0

    @Volatile var myId: String? = null
    private var joinedMembers: List<Map<String, Any?>> = emptyList()
    /** 房间是否开放（服务器 ver>=2 才带此字段）。null = 服务器未提供。 */
    @Volatile var roomOpen: Boolean? = null
        private set
    /** 服务器协议版本（旧服务器默认 1）。 */
    @Volatile var serverVer: Int = 1
        private set

    /**
     * 房间级拒绝码（服务器明确回了 error）：非空表示"服务器可达但拒绝加入"，
     * 区别于"超时无响应"。房间级拒绝【不触发】多服务器故障转移。
     */
    @Volatile var lastJoinError: String? = null
        private set

    @Volatile private var sock: DatagramSocket? = null
    @Volatile private var mapSock: Socket? = null
    @Volatile private var running = false
    private var recvThread: Thread? = null
    private var hbThread: Thread? = null

    @Volatile var udpHoleSock: DatagramSocket? = null
        private set
    private var udpHoleThread: Thread? = null
    private val udpRtpHadlers = HashMap<String, (ByteArray) -> Unit>()
    private val udpRtpLock = ReentrantLock()
    @Volatile private var udpRecvRunning = false
    private var udpRecvThread: Thread? = null
    private val holeTargets = HashMap<String, String>()
    private val holeTargetsLock = ReentrantLock()

    @Volatile private var reuseId: String = ""

    init {
        var list = normalizeServers(servers, serverIp, serverPort,
            serverTcpPort ?: C.DEFAULT_SERVER_TCP_PORT,
            natProbePort ?: C.NAT_PROBE_PORT)
        var usingDefault = false
        if (list.isEmpty()) {
            list = Defaults.defaultServers()
            usingDefault = true
        }
        if (list.isEmpty()) throw IllegalArgumentException("SignalingClient 需要 server_ip 或 servers 之一")
        serverList = list
        usingDefaultServer = usingDefault
        this.serverIp = list[0].ip
        this.serverPort = list[0].port
        this.serverTcpPort = list[0].tcpPort
        this.natProbePort = list[0].natPort
    }

    // ---------- 生命周期 ----------

    fun start(): Boolean {
        if (running) return false
        if (usingDefaultServer) {
            try {
                val st = Defaults.checkDefaultServerExpiry()
                if (st.status == "expired") {
                    log("[信令] ⚠ 默认服务器已于 " + st.expires + " 过期。请改用自建服务器或备用服务器。")
                } else if (st.status == "soon") {
                    log("[信令] ⚠ 默认服务器将于 " + st.expires + " 到期（剩 " + st.daysLeft + " 天），建议尽早改用自建服务器。")
                }
            } catch (_: Exception) {}
        }
        running = true
        val n = serverList.size
        var lastErr = ""
        var joinRejected: String? = null
        for (idx in 0 until n) {
            val spec = serverList[idx]
            serverIdx = idx
            serverIp = spec.ip; serverPort = spec.port
            serverTcpPort = spec.tcpPort; natProbePort = spec.natPort
            if (idx > 0) log("[信令] 切换备用服务器 -> " + spec.ip + ":" + spec.port + "（第 " + (idx + 1) + "/" + n + " 个）")
            if (tryConnectOne()) return true
            // 房间级拒绝：换服务器也没用（同一房间同一密码），立即停止故障转移
            val rej = lastJoinError
            if (rej != null) { joinRejected = rej; break }
            lastErr = spec.ip + ":" + spec.port
            if (!autoFailover) break
        }
        running = false
        if (joinRejected != null) {
            // 服务器可达但明确拒绝：错误详情已由 onError 上报，
            // 【不再】误报"无法连接任何信令服务器"，也【不发】CONNECT_FAILED。
            log("[信令] 加入房间被拒绝（" + joinRejected + "），已停止尝试其他服务器")
            return false
        }
        log("[信令] 无法连接任何信令服务器（共 " + n + " 个候选，最后尝试 " + lastErr + "）。")
        onError?.invoke("CONNECT_FAILED")
        return false
    }

    private fun tryConnectOne(): Boolean {
        lastJoinError = null
        val ds = try {
            DatagramSocket().apply { soTimeout = (config.recvTimeout * 1000).toInt() }
        } catch (e: Exception) {
            log("[信令] 创建 socket 失败: " + e.message); return false
        }
        sock = ds
        sendJoin()
        if (!waitJoined()) {
            if (lastJoinError != null) {
                // 服务器可达但明确拒绝加入（密码错/房间满等）：
                // 换服务器也没用（同一房间同一密码），不发"无响应"误导日志。
                log("[信令] " + serverIp + ":" + serverPort + " 拒绝加入（" + lastJoinError + "）")
            } else {
                log("[信令] 连接 " + serverIp + ":" + serverPort + " 失败（" + connectTimeout.toInt() + " 秒内无响应）")
            }
            try { ds.close() } catch (_: Exception) {}
            sock = null
            return false
        }
        if (myId != null) {
            syncTime()
            openMapping()
            openUdpHoleSocket()
            Thread.sleep(200)
        }
        onJoined?.invoke(joinedMembers)
        recvThread = Thread({ recvLoop() }, "dpmp-sig-recv").apply { isDaemon = true; start() }
        hbThread = Thread({ hbLoop() }, "dpmp-sig-hb").apply { isDaemon = true; start() }
        log("[信令] 已加入房间 " + roomStr + "，我的ID=" + myId)
        Thread({ runNatProbe() }, "dpmp-sig-natprobe").apply { isDaemon = true; start() }
        return true
    }

    private fun runNatProbe() {
        try {
            val r = Natprobe.detectNatType(serverIp, serverPort, natProbePort, 1.5, log)
            val names = mapOf(
                "cone" to "锥形 NAT（Cone）",
                "symmetric" to "对称 NAT（Symmetric）",
                "no_udp" to "UDP 被封堵（无法探测）",
                "unknown" to "未知（仅收到一路回应）"
            )
            log("[NAT探测] 本机 NAT 类型 = " + (names[r.type] ?: r.type))
            log("[NAT探测] 探测 1（UDP " + serverPort + "）观察到: " + r.primary)
            log("[NAT探测] 探测 2（UDP " + natProbePort + "）观察到: " + r.alt)
            if (r.type == "symmetric") log("[NAT探测] 提示：本机为对称 NAT，TCP 打洞成功率极低。")
            else if (r.type == "cone") log("[NAT探测] 提示：本机为锥形 NAT，打洞可行性较高。")
        } catch (e: Exception) {
            log("[NAT探测] 异常: " + e.message)
        }
    }

    fun stop(quiet: Boolean = false) {
        if (!running) return
        running = false
        if (!quiet) {
            try {
                myId?.let { send(mapOf("type" to C.T_BYE, "ver" to C.DPMP_VER, "id" to it)) }
            } catch (_: Exception) {}
        }
        try { mapSock?.close() } catch (_: Exception) {}
        udpRecvRunning = false
        try { udpHoleSock?.close() } catch (_: Exception) {}
        try { sock?.close() } catch (_: Exception) {}
    }

    /** 网络切换后重建信令连接。 */
    fun rebind(): Boolean {
        if (!running) return false
        reuseId = myId ?: ""
        stop(quiet = true)
        myId = null
        joinedMembers = emptyList()
        udpRtpLock.lock(); try { udpRtpHadlers.clear() } finally { udpRtpLock.unlock() }
        holeTargetsLock.lock(); try { holeTargets.clear() } finally { holeTargetsLock.unlock() }
        return start()
    }

    // ---------- UDP 打洞与可靠通道 ----------

    private fun openUdpHoleSocket() {
        try {
            val ds = DatagramSocket(null)
            ds.reuseAddress = true
            ds.bind(InetSocketAddress(udpHolePort))
            ds.soTimeout = 1000
            udpHoleSock = ds
            val hello = Json.encode(mapOf(
                "type" to C.T_UDP_HELLO, "ver" to C.DPMP_VER, "id" to myId
            )).toByteArray(Charsets.UTF_8)
            ds.send(DatagramPacket(hello, hello.size, InetAddress.getByName(serverIp), serverPort))
            log("[UDP打洞] 打洞 socket 已绑定本地 " + udpHolePort + "，已向服务器登记映射")
            if (!udpRecvRunning) {
                udpRecvRunning = true
                udpRecvThread = Thread({ udpGlobalRecvLoop() }, "dpmp-udp-recv").apply { isDaemon = true; start() }
            }
            for (m in joinedMembers) {
                val mid = m["id"] as? String ?: continue
                try {
                    send(mapOf("type" to C.T_PUNCH_REQ, "ver" to C.DPMP_VER, "id" to myId, "target" to mid))
                } catch (_: Exception) {}
            }
        } catch (e: Exception) {
            log("[UDP打洞] socket 打开失败: " + e.message)
            udpHoleSock = null
        }
    }

    /** 向指定对端地址发送一个 UDP 包（UDP-RTP 发送入口）。 */
    fun sendUdpTo(peerAddr: Pair<String, Int>, data: ByteArray) {
        try {
            udpHoleSock?.send(DatagramPacket(data, data.size,
                InetAddress.getByName(peerAddr.first), peerAddr.second))
        } catch (e: Exception) {
            log("[UDP-RTP] send_udp_to 失败: " + e.message)
        }
    }

    fun registerUdpRtp(peerKey: String, onPacket: (ByteArray) -> Unit) {
        udpRtpLock.lock(); try { udpRtpHadlers[peerKey] = onPacket } finally { udpRtpLock.unlock() }
    }

    fun unregisterUdpRtp(peerKey: String) {
        udpRtpLock.lock(); try { udpRtpHadlers.remove(peerKey) } finally { udpRtpLock.unlock() }
    }

    private fun udpGlobalRecvLoop() {
        log("[UDP-RTP] 全局接收循环启动")
        val buf = ByteArray(2048)
        while (udpRecvRunning) {
            val ds = udpHoleSock ?: break
            try {
                ds.soTimeout = 500
                val pkt = DatagramPacket(buf, buf.size)
                ds.receive(pkt)
                val data = pkt.data.copyOfRange(0, pkt.length)
                val addr = pkt.address.hostAddress to pkt.port
                val key = addr.first + ":" + addr.second
                if (data.contentEquals(C.UDP_HOLE_MAGIC)) {
                    val peerId = holeTargetsLock.let { it.lock(); try { holeTargets[key] } finally { it.unlock() } }
                    if (peerId != null) {
                        val already = udpRtpLock.let { it.lock(); try { udpRtpHadlers.containsKey(key) } finally { it.unlock() } }
                        if (!already) {
                            log("[UDP打洞] ★ 成功！收到 " + key + " 的探测包（peer=" + peerId + "）")
                            try { onUdpHoleReady?.invoke(peerId, addr) } catch (ex: Exception) {
                                log("[UDP打洞] 回调异常: " + ex.message)
                            }
                        }
                    }
                    continue
                }
                val handler = udpRtpLock.let { it.lock(); try { udpRtpHadlers[key] } finally { it.unlock() } }
                handler?.let {
                    try { it(data) } catch (e: Exception) { log("[UDP-RTP] 处理包异常: " + e.message) }
                }
            } catch (_: SocketTimeoutException) {
                continue
            } catch (_: Exception) {
                break
            }
        }
        log("[UDP-RTP] 全局接收循环退出")
    }

    private fun startUdpHole(peer: Map<String, Any?>) {
        val ds = udpHoleSock
        if (ds == null) { log("[UDP打洞] socket 未打开，跳过"); return }
        val peerId = peer["id"] as? String ?: return
        val pubUdp = peer["pub_udp"] as? String ?: ""
        val parsed = Addr.parseHostPort(pubUdp)
        if (parsed == null) { log("[UDP打洞] 对端 pub_udp 无效（" + pubUdp + "），无法打洞"); return }
        val (ip, port) = parsed
        val key = ip + ":" + port
        holeTargetsLock.lock(); try { holeTargets[key] = peerId } finally { holeTargetsLock.unlock() }

        val probe = Thread({
            val tEnd = System.currentTimeMillis() + (config.udpProbeDuration * 1000).toLong()
            var sent = 0
            log("[UDP打洞] 开始向 " + key + " 发探测包（" + config.udpProbeDuration + " 秒）")
            while (System.currentTimeMillis() < tEnd) {
                if (!running) break
                try {
                    ds.send(DatagramPacket(C.UDP_HOLE_MAGIC, C.UDP_HOLE_MAGIC.size,
                        InetAddress.getByName(ip), port))
                    sent++
                } catch (e: Exception) {
                    log("[UDP打洞] 发送失败: " + e.message); break
                }
                Thread.sleep((config.udpProbeInterval * 1000).toLong())
            }
            val stillWaiting = holeTargetsLock.let { it.lock(); try { holeTargets[key] == peerId } finally { it.unlock() } }
            val hasHandler = udpRtpLock.let { it.lock(); try { udpRtpHadlers.containsKey(key) } finally { it.unlock() } }
            if (stillWaiting && !hasHandler) {
                holeTargetsLock.lock(); try { holeTargets.remove(key) } finally { holeTargetsLock.unlock() }
                log("[UDP打洞] 失败：发了 " + sent + " 个包，未收到 " + key + " 响应（peer=" + peerId + "）")
            }
        }, "dpmp-udp-hole")
        probe.isDaemon = true
        udpHoleThread = probe
        probe.start()
    }

    fun requestPunch(targetId: String) {
        send(mapOf("type" to C.T_PUNCH_REQ, "ver" to C.DPMP_VER, "id" to myId, "target" to targetId))
    }

    // ---------- 信令收发 ----------

    private fun send(obj: Map<String, Any?>) {
        val ds = sock ?: return
        val data = Json.encode(obj).toByteArray(Charsets.UTF_8)
        ds.send(DatagramPacket(data, data.size, InetAddress.getByName(serverIp), serverPort))
    }

    private fun sendJoin() {
        val msg = HashMap<String, Any?>()
        msg["type"] = C.T_JOIN; msg["ver"] = C.DPMP_VER; msg["room"] = roomStr
        msg["name"] = name; msg["tcp"] = tcpPort; msg["lan"] = lanIpList
        msg["did"] = deviceId
        if (reuseId.isNotEmpty()) msg["reuse_id"] = reuseId
        // 房间密码：仅在【有密码时】才带该字段；无密码时不发 pwd，
        // 报文与旧版完全一致 → 开放房间零兼容风险。
        if (password.isNotEmpty()) msg["pwd"] = password
        send(msg)
    }

    private fun syncTime() {
        val ds = sock ?: return
        var bestRtt: Long? = null
        var bestOffset = 0L
        val oldTimeout = try { ds.soTimeout } catch (_: Exception) { -1 }
        for (i in 0 until 4) {
            try {
                val t1 = System.currentTimeMillis()
                send(mapOf("type" to C.T_TIME_REQ, "ver" to C.DPMP_VER, "t1" to t1))
                ds.soTimeout = 1000
                val deadline = System.currentTimeMillis() + 1000
                var reply: Map<String, Any?>? = null
                while (System.currentTimeMillis() < deadline) {
                    val pkt: DatagramPacket
                    try {
                        val buf = ByteArray(65535)
                        pkt = DatagramPacket(buf, buf.size)
                        ds.receive(pkt)
                    } catch (_: SocketTimeoutException) {
                        break
                    }
                    val msg = try { Json.decode(String(pkt.data, 0, pkt.length, Charsets.UTF_8)) } catch (_: Exception) { continue }
                    if (msg["type"] == C.T_TIME_REPLY && (msg["t1"] as? Number)?.toLong() == t1) {
                        reply = msg; break
                    }
                }
                if (reply == null) continue
                val t3 = System.currentTimeMillis()
                val t2 = (reply["t2"] as? Number)?.toLong() ?: 0L
                val rtt = t3 - t1
                val offset = t2 - (t1 + t3) / 2
                if (bestRtt == null || rtt < bestRtt) { bestRtt = rtt; bestOffset = offset }
                Thread.sleep(50)
            } catch (e: Exception) {
                log("[校时] 采样失败: " + e.message); break
            }
        }
        try { ds.soTimeout = if (oldTimeout <= 0) (config.recvTimeout * 1000).toInt() else oldTimeout } catch (_: Exception) {}
        if (bestRtt != null) {
            clockOffsetMs = bestOffset
            log("[校时] 与服务器时钟偏差 = " + bestOffset + " ms（最小 RTT " + bestRtt + " ms）")
        } else {
            log("[校时] 未能同步，使用本地时钟（可能影响打洞时刻）")
        }
    }

    private fun openMapping() {
        for (attempt in 1..3) {
            var s: Socket? = null
            try {
                s = Socket()
                s.reuseAddress = true
                punchLocalPort?.let { s.bind(InetSocketAddress(it)) }
                s.connect(InetSocketAddress(serverIp, serverTcpPort), 10000)
                val mid = (myId ?: "").toByteArray(Charsets.UTF_8)
                val out = java.io.ByteArrayOutputStream()
                out.write((mid.size shr 8) and 0xFF); out.write(mid.size and 0xFF)
                out.write(mid)
                s.getOutputStream().write(out.toByteArray())
                s.getOutputStream().flush()
                mapSock = s
                log("[信令] TCP 映射观测连接已建立（本地端口=" + s.localPort + "）")
                try { onMappingReady?.invoke() } catch (e: Exception) {
                    log("[信令] on_mapping_ready 回调异常: " + e.message)
                }
                return
            } catch (e: Exception) {
                try { s?.close() } catch (_: Exception) {}
                if (attempt < 3) { Thread.sleep(500); continue }
                log("[信令] TCP 映射观测连接失败: " + e.message)
            }
        }
    }

    private fun waitJoined(): Boolean {
        val ds = sock ?: return false
        val deadline = System.currentTimeMillis() + (connectTimeout * 1000).toLong()
        while (System.currentTimeMillis() < deadline && running) {
            val pkt: DatagramPacket
            try {
                val buf = ByteArray(65535)
                pkt = DatagramPacket(buf, buf.size)
                ds.receive(pkt)
            } catch (_: SocketTimeoutException) {
                sendJoin(); continue
            } catch (_: Exception) {
                return false
            }
            val msg = try { Json.decode(String(pkt.data, 0, pkt.length, Charsets.UTF_8)) } catch (_: Exception) { continue }
            when (msg["type"]) {
                C.T_JOINED -> {
                    myId = msg["id"] as? String
                    @Suppress("UNCHECKED_CAST")
                    joinedMembers = (msg["members"] as? List<Map<String, Any?>>) ?: emptyList()
                    roomOpen = msg["room_open"] as? Boolean
                    serverVer = (msg["ver"] as? Number)?.toInt() ?: 1
                    return true
                }
                C.T_ERROR -> {
                    val code = msg["code"] as? String ?: "UNKNOWN"
                    lastJoinError = code
                    onError?.invoke(code)
                    return false
                }
            }
        }
        return false
    }

    private fun recvLoop() {
        val ds = sock ?: return
        while (running) {
            val pkt: DatagramPacket
            try {
                val buf = ByteArray(65535)
                pkt = DatagramPacket(buf, buf.size)
                ds.receive(pkt)
            } catch (_: SocketTimeoutException) {
                continue
            } catch (_: Exception) {
                break
            }
            val msg = try { Json.decode(String(pkt.data, 0, pkt.length, Charsets.UTF_8)) } catch (_: Exception) { continue }
            when (msg["type"]) {
                C.T_MEMBER_JOIN -> {
                    @Suppress("UNCHECKED_CAST")
                    onMemberJoin?.invoke((msg["member"] as? Map<String, Any?>) ?: emptyMap())
                }
                C.T_MEMBER_LEAVE -> onMemberLeave?.invoke(msg["id"] as? String ?: "")
                C.T_PUNCH_GO -> {
                    @Suppress("UNCHECKED_CAST")
                    val peer = (msg["peer"] as? Map<String, Any?>) ?: emptyMap()
                    try { startUdpHole(peer) } catch (e: Exception) { log("[UDP打洞] 启动异常: " + e.message) }
                    onPunchGo?.invoke(peer, (msg["at"] as? Number)?.toLong() ?: 0L)
                }
                C.T_ERROR -> onError?.invoke(msg["code"] as? String ?: "UNKNOWN")
            }
        }
    }

    private fun hbLoop() {
        while (running) {
            try { Thread.sleep(config.heartbeatInterval * 1000L) } catch (_: InterruptedException) { break }
            if (!running) break
            try { send(mapOf("type" to C.T_HB, "ver" to C.DPMP_VER, "id" to myId)) } catch (_: Exception) {}
        }
    }

    companion object {
        private fun udpPortOrDefault(p: Int?): Int = p ?: C.ROOM_UDP_PORT

        private fun normalizeServers(
            servers: List<Any>?, defaultIp: String?, defaultPort: Int?,
            defaultTcp: Int, defaultNat: Int
        ): List<ServerSpec> {
            val tcp = defaultTcp
            val nat = defaultNat
            val out = ArrayList<ServerSpec>()
            if (servers != null) {
                for (s in servers) {
                    parseServerSpec(s, tcp, nat)?.let { out.add(it) }
                }
            }
            if (out.isEmpty()) {
                val ip = defaultIp
                val port = defaultPort ?: C.DEFAULT_SERVER_PORT
                if (ip != null) out.add(ServerSpec(ip, port, tcp, nat))
            }
            return out
        }

        /** 支持 "ip:port"、ServerSpec、Map、List。 */
        @Suppress("UNCHECKED_CAST")
        private fun parseServerSpec(spec: Any, defaultTcp: Int, defaultNat: Int): ServerSpec? {
            var ip: String? = null
            var port: Int? = null
            var tcp = defaultTcp
            var nat = defaultNat
            when (spec) {
                is String -> {
                    val parsed = Addr.parseHostPort(spec) ?: return null
                    ip = parsed.first; port = parsed.second
                }
                is ServerSpec -> {
                    ip = spec.ip; port = spec.port; tcp = spec.tcpPort; nat = spec.natPort
                }
                is Map<*, *> -> {
                    ip = spec["ip"] as? String
                    port = (spec["port"] as? Number)?.toInt()
                    (spec["tcp_port"] as? Number)?.let { tcp = it.toInt() }
                    (spec["nat_probe_port"] as? Number)?.let { nat = it.toInt() }
                }
                is List<*> -> {
                    if (spec.size >= 2) { ip = spec[0] as? String; port = (spec[1] as? Number)?.toInt() }
                    if (spec.size >= 3) (spec[2] as? Number)?.let { tcp = it.toInt() }
                    if (spec.size >= 4) (spec[3] as? Number)?.let { nat = it.toInt() }
                }
            }
            if (ip.isNullOrEmpty() || port == null) return null
            return ServerSpec(ip!!, port!!, tcp, nat)
        }
    }
}
