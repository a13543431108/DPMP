package io.dpmp.link

import io.dpmp.Config
import io.dpmp.protocol.C
import io.dpmp.protocol.Json
import io.dpmp.util.Net
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketTimeoutException
import java.util.concurrent.locks.ReentrantLock

/**
 * 局域网设备发现：UDP 广播 + 主动扫描 + 心跳。
 *
 * 节点按 device_id 去重（IP 变化不产生重复条目）。
 *
 * 端口：9998 UDP 广播/心跳；9997 UDP 主动扫描。
 *
 * 与 Python 端 dpmp/link/discovery.py 对齐。
 */
class Discovery(
    deviceId: String,
    hostname: String? = null,
    private val log: (String) -> Unit = {},
    private val onNewNode: ((String, Map<String, Any?>) -> Unit)? = null,
    private val onNodeGone: ((String) -> Unit)? = null,
    udpPort: Int? = null,
    scanPort: Int? = null,
    private val config: Config = Config.DEFAULT
) {
    private val discoverInterval = config.discoverInterval
    val deviceId: String = deviceId
    val hostname: String = hostname ?: try { InetAddress.getLocalHost().hostName } catch (_: Exception) { "unknown" }
    private val mac: String = Net.getMacAddress()

    val udpPort: Int = udpPort ?: C.LAN_UDP_PORT
    val scanPort: Int = scanPort ?: C.SCAN_PORT

    val myIps: List<String> = Net.getAllLocalIps(false)
    val myIpsV6: List<String> = Net.getAllLocalIps(true)
    private val broadcastAddrs: List<String> = Net.getBroadcastAddrs()

    private val nodes = HashMap<String, MutableMap<String, Any?>>()
    private val lock = ReentrantLock()
    @Volatile private var running = false
    private val broadcastSockets = ArrayList<DatagramSocket>()
    private val listenSockets = ArrayList<DatagramSocket>()
    @Volatile var autoScanEnabled = false
    @Volatile var scanning = false
    @Volatile var pausingNetwork = false

    // ---------- 生命周期 ----------

    fun start() {
        running = true
        Thread({ udpListener() }, "dpmp-disc-udp").apply { isDaemon = true; start() }
        Thread({ scanListener() }, "dpmp-disc-scan").apply { isDaemon = true; start() }
        startBroadcasters()
        log("本机 IPv4: " + myIps.joinToString(", "))
        if (myIpsV6.isNotEmpty()) log("本机 IPv6: " + myIpsV6.joinToString(", "))
        log("广播地址: " + broadcastAddrs.joinToString(", "))
    }

    fun stop() {
        running = false
        for (s in broadcastSockets + listenSockets) {
            try { s.close() } catch (_: Exception) {}
        }
        sendBye()
    }

    // ---------- 消息构造 ----------

    private fun buildMsg(vararg extra: Pair<String, Any?>): ByteArray {
        val msg = LinkedHashMap<String, Any?>()
        msg[C.LAN_F_HOSTNAME] = hostname
        msg[C.LAN_F_DEVICE_ID] = deviceId
        msg[C.LAN_F_MAC] = mac
        for ((k, v) in extra) msg[k] = v
        return Json.encode(msg).toByteArray(Charsets.UTF_8)
    }

    /** 按 device_id 去重地插入/更新节点。返回 true 表示新节点。 */
    private fun upsertNode(remoteIp: String, msg: Map<String, Any?>, source: String = "udp"): Boolean {
        val did = msg[C.LAN_F_DEVICE_ID] as? String ?: ""
        val hn = msg[C.LAN_F_HOSTNAME] as? String ?: remoteIp
        val m = msg[C.LAN_F_MAC] as? String ?: ""
        var isNew = false
        lock.lock()
        try {
            if (did.isNotEmpty()) {
                for (oldIp in nodes.keys.toList()) {
                    if (oldIp == remoteIp) continue
                    if (nodes[oldIp]?.get(C.LAN_F_DEVICE_ID) == did) nodes.remove(oldIp)
                }
            }
            val existing = nodes[remoteIp]
            if (existing != null) {
                existing[C.LAN_F_HOSTNAME] = hn
                existing[C.LAN_F_DEVICE_ID] = did
                existing[C.LAN_F_MAC] = m
                existing["last_seen"] = nowSec()
                existing["heartbeat_fail"] = 0
            } else {
                val n = HashMap<String, Any?>()
                n[C.LAN_F_HOSTNAME] = hn; n[C.LAN_F_DEVICE_ID] = did; n[C.LAN_F_MAC] = m
                n["last_seen"] = nowSec(); n["source"] = source; n["heartbeat_fail"] = 0
                nodes[remoteIp] = n
                isNew = true
            }
        } finally {
            lock.unlock()
        }
        return isNew
    }

    fun getNodes(): Map<String, Map<String, Any?>> {
        lock.lock(); try { return nodes.mapValues { HashMap(it.value) } } finally { lock.unlock() }
    }

    // ---------- 广播 ----------

    private fun startBroadcasters() {
        for (ipStr in myIps) {
            try {
                val s = DatagramSocket(null)
                s.broadcast = true
                s.reuseAddress = true
                s.bind(InetSocketAddress(ipStr, 0))
                broadcastSockets.add(s)
                Thread({ udpBroadcasterOnSock(s) }, "dpmp-disc-bcast").apply { isDaemon = true; start() }
            } catch (e: Exception) {
                log("[警告] 无法为 " + ipStr + " 创建广播 socket: " + e.message)
            }
        }
        try {
            val g = DatagramSocket(null)
            g.broadcast = true
            g.reuseAddress = true
            broadcastSockets.add(g)
            Thread({ udpBroadcasterOnSock(g) }, "dpmp-disc-bcast-global").apply { isDaemon = true; start() }
        } catch (e: Exception) {
            log("[警告] 全局广播 socket 创建失败: " + e.message)
        }
    }

    private fun udpBroadcasterOnSock(sock: DatagramSocket) {
        val start = System.currentTimeMillis()
        while (running) {
            if (!pausingNetwork) {
                try {
                    val msg = buildMsg(C.LAN_K_DISCOVERY to true)
                    sock.send(DatagramPacket(msg, msg.size,
                        InetAddress.getByName("255.255.255.255"), udpPort))
                } catch (_: Exception) {}
            }
            val elapsed = (System.currentTimeMillis() - start) / 1000.0
            Thread.sleep(if (elapsed < 5) 200 else discoverInterval * 1000L)
        }
    }

    fun broadcastSearch() {
        log("[广播搜索] 发送广播探测，等待设备回复...")
        val msg = buildMsg(C.LAN_K_DISCOVERY to true)
        for (r in 0 until 3) {
            for (bcast in broadcastAddrs) {
                try {
                    val s = DatagramSocket()
                    s.broadcast = true
                    s.soTimeout = 1000
                    s.send(DatagramPacket(msg, msg.size, InetAddress.getByName(bcast), udpPort))
                    s.close()
                } catch (_: Exception) {}
            }
            Thread.sleep(200)
        }
        log("[广播搜索] 完成")
    }

    // ---------- 接收 ----------

    private fun udpListener() {
        val sock4: DatagramSocket
        try {
            sock4 = DatagramSocket(null)
            sock4.reuseAddress = true
            sock4.bind(InetSocketAddress(udpPort))
            sock4.soTimeout = 1000
            listenSockets.add(sock4)
        } catch (e: Exception) {
            log("[发现] UDP 监听失败: " + e.message); return
        }
        var sock6: DatagramSocket? = null
        try {
            sock6 = DatagramSocket(null)
            sock6.reuseAddress = true
            sock6.bind(InetSocketAddress("::", udpPort))
            sock6.soTimeout = 1000
            listenSockets.add(sock6)
        } catch (_: Exception) { sock6 = null }

        Thread({ listenSock(sock4) }, "dpmp-disc-listen4").apply { isDaemon = true; start() }
        sock6?.let { Thread({ listenSock(it) }, "dpmp-disc-listen6").apply { isDaemon = true; start() } }
    }

    private fun listenSock(sock: DatagramSocket) {
        val buf = ByteArray(1024)
        while (running) {
            try {
                val pkt = DatagramPacket(buf, buf.size)
                sock.receive(pkt)
                val msg = try { Json.decode(String(pkt.data, 0, pkt.length, Charsets.UTF_8)) } catch (_: Exception) { continue }
                val remoteIp = pkt.address.hostAddress
                if (remoteIp in myIps || remoteIp in myIpsV6) continue
                if (remoteIp == "127.0.0.1" || remoteIp == "::1" || remoteIp == "0.0.0.0") continue
                val did = msg[C.LAN_F_DEVICE_ID] as? String ?: ""
                if (did.isNotEmpty() && did == deviceId) continue
                if (msg[C.LAN_K_BYE] == true) {
                    val removed = lock.let { it.lock(); try { nodes.remove(remoteIp) != null } finally { it.unlock() } }
                    if (removed) try { onNodeGone?.invoke(remoteIp) } catch (_: Exception) {}
                    continue
                }
                if (msg[C.LAN_K_HEARTBEAT] == true) {
                    lock.lock(); try { nodes[remoteIp]?.put("last_seen", nowSec()) } finally { lock.unlock() }
                    val reply = buildMsg(C.LAN_K_HEARTBEAT to true, C.LAN_K_ACK to true)
                    sock.send(DatagramPacket(reply, reply.size, InetAddress.getByName(remoteIp), udpPort))
                    continue
                }
                val isNew = upsertNode(remoteIp, msg, "udp")
                if (isNew) {
                    val hn = msg[C.LAN_F_HOSTNAME] as? String ?: remoteIp
                    log("[发现] 新设备 " + hn + " (" + remoteIp + ")")
                    try { onNewNode?.invoke(remoteIp, msg) } catch (_: Exception) {}
                }
                if (msg[C.LAN_K_REPLY] != true) {
                    val reply = buildMsg(C.LAN_K_REPLY to true)
                    sock.send(DatagramPacket(reply, reply.size, InetAddress.getByName(remoteIp), udpPort))
                }
            } catch (_: SocketTimeoutException) {
                continue
            } catch (_: Exception) {
                break
            }
        }
        try { sock.close() } catch (_: Exception) {}
    }

    // ---------- 扫描 ----------

    private fun scanListener() {
        val sock: DatagramSocket
        try {
            sock = DatagramSocket(null)
            sock.reuseAddress = true
            sock.bind(InetSocketAddress(scanPort))
            sock.soTimeout = 1000
            listenSockets.add(sock)
        } catch (e: Exception) {
            log("[扫描] 监听失败: " + e.message); return
        }
        val buf = ByteArray(1024)
        while (running) {
            try {
                val pkt = DatagramPacket(buf, buf.size)
                sock.receive(pkt)
                val msg = try { Json.decode(String(pkt.data, 0, pkt.length, Charsets.UTF_8)) } catch (_: Exception) { continue }
                val remoteIp = pkt.address.hostAddress
                val did = msg[C.LAN_F_DEVICE_ID] as? String ?: ""
                if (did.isNotEmpty() && did == deviceId) continue
                if (remoteIp in myIps || remoteIp == "127.0.0.1" || remoteIp == "::1") continue
                val reply = buildMsg(C.LAN_K_SCAN_REPLY to true)
                sock.send(DatagramPacket(reply, reply.size, InetAddress.getByName(remoteIp), scanPort))
                val isNew = upsertNode(remoteIp, msg, "scan")
                if (isNew) try { onNewNode?.invoke(remoteIp, msg) } catch (_: Exception) {}
            } catch (_: SocketTimeoutException) {
                continue
            } catch (_: Exception) {
                break
            }
        }
        try { sock.close() } catch (_: Exception) {}
    }

    /** 扫描一个子网（IPv4），发现的对端通过 onNewNode 回调。 */
    fun scanSubnet(networkCidr: String, callback: ((String) -> Unit)? = null) {
        val range = cidrHosts(networkCidr) ?: return
        scanning = true
        val msg = buildMsg(C.LAN_K_SCAN_REPLY to true)
        var sock: DatagramSocket? = null
        try {
            sock = DatagramSocket()
            sock.soTimeout = 300
            for (host in range) {
                if (!running) break
                try {
                    sock.send(DatagramPacket(msg, msg.size, InetAddress.getByName(host), scanPort))
                } catch (_: Exception) {}
            }
            callback?.invoke(networkCidr)
        } catch (e: Exception) {
            log("[扫描] 子网 " + networkCidr + " 扫描异常: " + e.message)
        } finally {
            try { sock?.close() } catch (_: Exception) {}
            scanning = false
        }
    }

    /** 手动添加节点，并主动发探测让对方也发现我们。 */
    fun addManualNode(ip: String, hostname: String? = null) {
        lock.lock()
        try {
            val n = HashMap<String, Any?>()
            n["hostname"] = hostname ?: ip
            n["last_seen"] = nowSec(); n["source"] = "manual"; n["heartbeat_fail"] = 0
            nodes[ip] = n
        } finally {
            lock.unlock()
        }
        sendProbeTo(ip)
        log("[手动添加] 已向 " + ip + " 发送探测")
    }

    private fun sendProbeTo(ip: String) {
        try {
            val s = DatagramSocket()
            s.soTimeout = 2000
            val m1 = buildMsg()
            s.send(DatagramPacket(m1, m1.size, InetAddress.getByName(ip), udpPort))
            try {
                val m2 = buildMsg(C.LAN_K_SCAN_REPLY to true)
                s.send(DatagramPacket(m2, m2.size, InetAddress.getByName(ip), scanPort))
            } catch (_: Exception) {}
            s.close()
        } catch (e: Exception) {
            log("[手动添加] 向 " + ip + " 发送探测失败: " + e.message)
        }
    }

    private fun sendBye() {
        try {
            val s = DatagramSocket()
            s.broadcast = true
            val m = buildMsg(C.LAN_K_BYE to true)
            s.send(DatagramPacket(m, m.size, InetAddress.getByName("255.255.255.255"), udpPort))
            s.close()
        } catch (_: Exception) {}
    }

    companion object {
        private fun nowSec(): Double = System.nanoTime() / 1_000_000_000.0

        /** 生成 CIDR 内的所有主机 IP（不含网络号与广播地址）。 */
        private fun cidrHosts(cidr: String): List<String>? {
            val slash = cidr.indexOf('/')
            if (slash < 0) return null
            val ip = cidr.substring(0, slash)
            val prefix = cidr.substring(slash + 1).toIntOrNull() ?: return null
            val octets = ip.split(".").map { it.toIntOrNull() ?: return null }
            if (octets.size != 4) return null
            var addr = 0L
            for (o in octets) addr = (addr shl 8) or o.toLong()
            val mask = if (prefix == 0) 0L else (0xFFFFFFFFL shl (32 - prefix)) and 0xFFFFFFFFL
            val network = addr and mask
            val broadcast = network or (mask.inv() and 0xFFFFFFFFL)
            val out = ArrayList<String>()
            var cur = network + 1
            var count = 0
            while (cur < broadcast && count < 65536) {
                out.add(listOf(
                    (cur shr 24) and 0xFF, (cur shr 16) and 0xFF,
                    (cur shr 8) and 0xFF, cur and 0xFF
                ).joinToString("."))
                cur++
                count++
            }
            return out
        }
    }
}
