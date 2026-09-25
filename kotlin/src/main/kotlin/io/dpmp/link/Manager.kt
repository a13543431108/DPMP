package io.dpmp.link

import io.dpmp.Config
import io.dpmp.protocol.C
import io.dpmp.stream.DpmpSocket
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/** 单条到某成员的长连接。 */
class Conn(val member: Map<String, Any?>) {
    @Volatile var state: String = C.STATE_CONNECTING
    @Volatile var sock: DpmpSocket? = null
    @Volatile var addr: Pair<String, Int>? = null
    val ioLock = ReentrantLock()
}

/**
 * 房间连接管理。
 *
 * 把「双打洞协同」和「梯度冗余多路径」落到运行时。
 *
 * 与 Python 端 dpmp/link/manager.py 对齐。
 *
 * 回调：
 *   onSocketReady(peerId, sock, member)   TCP 通道建立
 *   onUdpReady(peerId, rtp)               UDP-RTP 通道建立
 *   onStateChanged()                      成员状态变化
 */
class LinkManager(
    val signaling: SignalingClient?,
    val localTcpPort: Int,
    private val log: (String) -> Unit = {},
    private val config: Config = Config.DEFAULT
) {
    private val lock = ReentrantLock()
    private val members = HashMap<String, MutableMap<String, Any?>>()
    private val connections = HashMap<String, Conn>()
    private val udpConns = HashMap<String, UdpReliableSocket>()
    private val punchSem = Semaphore(config.punchConcurrency)
    private val puncher = HolePuncher(localTcpPort, log, config = config)
    @Volatile private var running = false
    private var keepaliveThread: Thread? = null

    var onSocketReady: ((String, DpmpSocket, Map<String, Any?>) -> Unit)? = null
    var onUdpReady: ((String, UdpReliableSocket) -> Unit)? = null
    var onStateChanged: (() -> Unit)? = null

    private val rebuildCooldown = HashMap<String, Double>()
    private val rebuildCooldownLock = ReentrantLock()
    private val rebuildCooldownSec = config.rebuildCooldownSec

    private val punchRoundCond = ReentrantLock()
    private val punchRoundCondVar = punchRoundCond.newCondition()
    private val punchRoundEvt = HashMap<String, Pair<Long, Long>>()

    @Volatile private var mappingReady = false
    private val pathSchedulers = HashMap<String, PeerPathScheduler>()
    private val punchActive = HashMap<String, Int>()
    private val peerLastActive = HashMap<String, Double>()
    private val recvEpoch = HashMap<String, Int>()

    @Volatile private var networkRebindLast = 0.0

    // ---------- 生命周期 ----------

    fun start() {
        running = true
        keepaliveThread = Thread({ keepaliveLoop() }, "dpmp-lm-keepalive").apply { isDaemon = true; start() }
    }

    fun stop() {
        running = false
        lock.lock()
        try {
            for (conn in connections.values) closeSock(conn)
            connections.clear()
            for (rtp in udpConns.values) { try { rtp.close() } catch (_: Exception) {} }
            udpConns.clear()
        } finally {
            lock.unlock()
        }
    }

    // ---------- 路径调度 ----------

    private fun getPathSched(peerId: String): PeerPathScheduler {
        lock.lock()
        try {
            return pathSchedulers.getOrPut(peerId) { PeerPathScheduler(peerId, log, config) }
        } finally {
            lock.unlock()
        }
    }

    fun markMappingReady() {
        mappingReady = true
        broadcastTcpReady()
    }

    // ---------- 双打洞：UDP 通道建立 ----------

    fun onUdpHoleReady(peerId: String, peerAddr: Pair<String, Int>) {
        val sig = signaling
        if (sig == null) { log("[UDP-RTP] signaling 为空，跳过"); return }
        lock.lock()
        try { if (udpConns.containsKey(peerId)) return } finally { lock.unlock() }

        val rtp: UdpReliableSocket
        try {
            try { puncher.setUdpHint(peerId, peerAddr.first, peerAddr.second) } catch (_: Exception) {}

            rtp = UdpReliableSocket(
                sendFn = { addr, data -> sig.sendUdpTo(addr, data) },
                peer = peerAddr,
                log = log,
                config = config,
                onPeerDead = { _ -> handleUdpPeerDead(peerId) },
                onSyncReady = { tGo ->
                    try { rtp_getRtt()?.let { rtt -> if (rtt > 0) puncher.setAdaptiveTimeout(rtt) } } catch (_: Exception) {}
                    onSyncReadyInternal(peerId, tGo)
                },
                onPunchRound = { roundNo, tGoR -> onPunchRoundInternal(peerId, roundNo, tGoR) },
                onTcpReady = { onPeerTcpReady(peerId) },
                onPunchFail = { onPeerPunchFail(peerId) }
            )
        } catch (e: Exception) {
            log("[UDP-RTP] 创建失败: " + e.message); return
        }

        // rtp_getRtt 引用 rtp，需要延迟绑定
        rtpRef = rtp

        try {
            val peerKey = peerAddr.first + ":" + peerAddr.second
            sig.registerUdpRtp(peerKey) { data -> rtp.onPacket(data) }
        } catch (e: Exception) {
            log("[UDP-RTP] 注册接收回调失败: " + e.message); return
        }

        val member: Map<String, Any?>
        lock.lock()
        try {
            udpConns[peerId] = rtp
            member = HashMap(members[peerId] ?: emptyMap())
        } finally {
            lock.unlock()
        }
        log("[UDP-RTP] 已为 " + (member["name"] ?: peerId) + " 建立可靠通道 " + peerAddr)
        try { getPathSched(peerId).register("udp:" + peerId, "udp", 0) } catch (_: Exception) {}
        try { onUdpReady?.invoke(peerId, rtp) } catch (e: Exception) { log("[UDP-RTP] 回调异常: " + e.message) }
        if (mappingReady) {
            try { rtp.sendTcpReady(); log("[打洞] 新通道通知 TCP_READY -> peer=" + peerId) } catch (_: Exception) {}
        }
        try {
            val myId = sig.myId ?: ""
            if (myId.isNotEmpty() && myId < peerId) {
                Thread({ maybeStartSync(peerId, rtp) }, "dpmp-lm-sync").apply { isDaemon = true; start() }
            }
        } catch (e: Exception) {
            log("[SYNC] 启动判断异常: " + e.message)
        }
    }

    @Volatile private var rtpRef: UdpReliableSocket? = null
    private fun rtp_getRtt(): Long? = rtpRef?.getSyncRtt()

    private fun maybeStartSync(peerId: String, rtp: UdpReliableSocket) {
        try {
            Thread.sleep(300)
            lock.lock()
            val conn = try { connections[peerId] } finally { lock.unlock() }
            if (conn != null && conn.state == C.STATE_CONNECTED) return
            log("[SYNC] 作为 initiator 向 peer=" + peerId + " 发起 SYNC")
            rtp.startSync()
        } catch (e: Exception) {
            log("[SYNC] 异常: " + e.message)
        }
    }

    private fun onSyncReadyInternal(peerId: String, tGo: Long) {
        val peer: Map<String, Any?>
        lock.lock()
        try {
            val conn = connections[peerId]
            if (conn != null && conn.state == C.STATE_CONNECTED) return
            peer = members[peerId] ?: return
        } finally {
            lock.unlock()
        }
        Thread({ syncPunchTask(peerId, peer, tGo) }, "dpmp-lm-syncpunch").apply { isDaemon = true; start() }
    }

    private fun syncPunchTask(peerId: String, peer: Map<String, Any?>, tGo: Long) {
        try {
            val nowMs = System.currentTimeMillis()
            val waitSec = (tGo - nowMs) / 1000.0
            if (waitSec > 0) Thread.sleep(minOf(waitSec, 3.0).toLong().coerceAtLeast(0))
            val result = puncher.punch(peer, 0L)
            if (result != null) {
                installSocket(peerId, result)
                log("[SYNC] 精准 TCP 打洞成功 peer=" + peerId)
            } else {
                log("[SYNC] 精准 TCP 打洞失败 peer=" + peerId + "，转入多轮重试")
                punchTask(peer, 0L)
            }
        } catch (e: Exception) {
            log("[SYNC] TCP 打洞异常: " + e.message)
        }
    }

    fun getUdpSocket(peerId: String): UdpReliableSocket? {
        lock.lock(); try { return udpConns[peerId] } finally { lock.unlock() }
    }

    fun broadcastTcpReady() {
        val items: List<Pair<String, UdpReliableSocket>>
        lock.lock(); try { items = udpConns.map { it.key to it.value } } finally { lock.unlock() }
        for ((pid, rtp) in items) {
            try { rtp.sendTcpReady(); log("[打洞] 广播 TCP_READY -> peer=" + pid) } catch (_: Exception) {}
        }
    }

    private fun onPeerTcpReady(peerId: String) {
        val peer: Map<String, Any?>
        lock.lock()
        try {
            peer = members[peerId] ?: return
            val conn = connections[peerId]
            if (conn != null && (conn.state == C.STATE_CONNECTING || conn.state == C.STATE_CONNECTED)) return
            connections[peerId] = Conn(peer)
        } finally {
            lock.unlock()
        }
        log("[打洞] 对端 TCP 就绪，立即发起打洞 peer=" + peerId)
        Thread({ punchTask(peer, 0L) }, "dpmp-lm-punch").apply { isDaemon = true; start() }
    }

    private fun onPeerPunchFail(peerId: String) {
        log("[打洞] 对端放弃 TCP，peer=" + peerId + " 关闭半开连接并回退 UDP")
        lock.lock()
        try {
            val conn = connections[peerId]
            if (conn != null) {
                closeSock(conn)
                conn.state = C.STATE_FAILED
            }
        } finally {
            lock.unlock()
        }
        try { onPathFailure(peerId, "tcp") } catch (_: Exception) {}
        try { onStateChanged?.invoke() } catch (_: Exception) {}
    }

    private fun onPunchRoundInternal(peerId: String, roundNo: Long, tGoR: Long) {
        punchRoundCond.lock()
        try {
            punchRoundEvt[peerId] = roundNo to tGoR
            punchRoundCondVar.signalAll()
        } finally {
            punchRoundCond.unlock()
        }
    }

    private fun waitPunchRound(peerId: String, roundNo: Long, timeoutSec: Double): Long? {
        val deadline = System.currentTimeMillis() + (timeoutSec * 1000).toLong()
        punchRoundCond.lock()
        try {
            while (true) {
                val v = punchRoundEvt[peerId]
                if (v != null && v.first >= roundNo) return v.second
                val remain = deadline - System.currentTimeMillis()
                if (remain <= 0) return null
                punchRoundCondVar.await(remain, TimeUnit.MILLISECONDS)
            }
        } finally {
            punchRoundCond.unlock()
        }
    }

    private fun sleepUntilLocal(tLocalMs: Long) {
        val remain = (tLocalMs - System.currentTimeMillis()) / 1000.0
        if (remain > 0) Thread.sleep(minOf(remain, 3.0).toLong().coerceAtLeast(1))
    }

    private fun onPathFailure(peerId: String, proto: String) {
        val sched = pathSchedulers[peerId] ?: return
        if (proto == "tcp") {
            val newHot = sched.promoteOnHotFailure()
            if (newHot != null) log("[路径] peer=" + peerId + " 热备失效，" + newHot + " 上位为热备")
        } else {
            sched.remove("udp:" + peerId)
        }
    }

    fun newRecvEpoch(peerKey: String): Int {
        lock.lock(); try {
            val e = (recvEpoch[peerKey] ?: 0) + 1
            recvEpoch[peerKey] = e
            return e
        } finally { lock.unlock() }
    }

    fun isCurrentEpoch(peerKey: String, epoch: Int): Boolean {
        lock.lock(); try { return recvEpoch[peerKey] == epoch } finally { lock.unlock() }
    }

    fun onNetworkChanged() {
        lock.lock()
        try { if (!running) return } finally { lock.unlock() }
        val now = nowSec()
        rebuildCooldownLock.lock()
        try {
            if (now - networkRebindLast < 10.0) return
            networkRebindLast = now
        } finally {
            rebuildCooldownLock.unlock()
        }
        log("[网络] 检测到网络切换，重建所有连接")
        lock.lock()
        try {
            for (conn in connections.values) closeSock(conn)
            connections.clear()
            for (rtp in udpConns.values) { try { rtp.close() } catch (_: Exception) {} }
            udpConns.clear()
            pathSchedulers.clear()
        } finally {
            lock.unlock()
        }
        signaling?.let {
            try { it.rebind() } catch (e: Exception) { log("[网络] 信令重建失败: " + e.message) }
        }
        val pids: List<String>
        lock.lock(); try { pids = members.keys.toList() } finally { lock.unlock() }
        rebuildCooldownLock.lock(); try { rebuildCooldown.clear() } finally { rebuildCooldownLock.unlock() }
        for (pid in pids) {
            try { requestRebuild(pid) } catch (_: Exception) {}
        }
    }

    private fun requestRebuild(peerId: String) {
        val now = nowSec()
        rebuildCooldownLock.lock()
        try {
            val last = rebuildCooldown[peerId] ?: 0.0
            if (now - last < rebuildCooldownSec) {
                log("[房间] peer=" + peerId + " 重建冷却中（" + (now - last).toInt() + " 秒前）")
                return
            }
            rebuildCooldown[peerId] = now
        } finally {
            rebuildCooldownLock.unlock()
        }
        try {
            signaling?.let { log("[房间] peer=" + peerId + " 请求重新打洞"); it.requestPunch(peerId) }
        } catch (e: Exception) {
            log("[房间] peer=" + peerId + " 重新打洞请求失败: " + e.message)
        }
    }

    private fun handleUdpPeerDead(peerId: String) {
        log("[UDP-RTP] peer=" + peerId + " 通道失联，清理并触发重建")
        try { onPathFailure(peerId, "udp") } catch (_: Exception) {}
        val rtp: UdpReliableSocket?
        lock.lock(); try { rtp = udpConns.remove(peerId) } finally { lock.unlock() }
        if (rtp != null) {
            try { rtp.close() } catch (_: Exception) {}
            try {
                val addr = rtp.peer
                val peerKey = addr.first + ":" + addr.second
                signaling?.unregisterUdpRtp(peerKey)
            } catch (_: Exception) {}
        }
        requestRebuild(peerId)
    }

    // ---------- 成员事件 ----------

    fun onJoined(membersList: List<Map<String, Any?>>) {
        for (m in membersList) addMember(m)
    }

    fun onMemberJoin(member: Map<String, Any?>) { addMember(member) }

    fun onMemberLeave(peerId: String) {
        val rtp: UdpReliableSocket?
        lock.lock()
        try {
            members.remove(peerId)
            connections.remove(peerId)?.let { closeSock(it) }
            pathSchedulers.remove(peerId)
            peerLastActive.remove(peerId)
            rtp = udpConns.remove(peerId)
        } finally {
            lock.unlock()
        }
        if (rtp != null) { try { rtp.close() } catch (_: Exception) {} }
        rebuildCooldownLock.lock(); try { rebuildCooldown.remove(peerId) } finally { rebuildCooldownLock.unlock() }
        log("[房间] 成员离开 " + peerId)
    }

    fun onPunchGo(peer: Map<String, Any?>, atMs: Long) {
        val peerId = peer["id"] as? String ?: return
        lock.lock()
        try {
            members[peerId] = HashMap(peer)
            val conn = connections[peerId]
            if (conn != null && (conn.state == C.STATE_CONNECTING || conn.state == C.STATE_CONNECTED)) return
            connections[peerId] = Conn(peer)
        } finally {
            lock.unlock()
        }
        Thread({ punchTask(peer, atMs) }, "dpmp-lm-punch-go").apply { isDaemon = true; start() }
    }

    /** 处理 listener accept 到的连接：若匹配成员 TCP 公网映射则接管。 */
    fun onInbound(channel: SocketChannel, addr: Pair<String, Int>): Boolean {
        val key = addr.first + ":" + addr.second
        var readyPid: String? = null
        var readyMember: Map<String, Any?>? = null
        lock.lock()
        try {
            for ((pid, m) in members) {
                val pubTcp = m["pub_tcp"] as? String ?: ""
                if (pubTcp.isNotEmpty() && pubTcp == key) {
                    val conn = connections[pid]
                    if (conn != null && conn.state == C.STATE_CONNECTED && conn.sock != null) return true
                    if (conn != null) closeSock(conn)
                    val newConn = Conn(m)
                    newConn.state = C.STATE_CONNECTED
                    newConn.sock = SocketChannelAdapter(channel, addr)
                    newConn.addr = addr
                    connections[pid] = newConn
                    readyPid = pid
                    readyMember = HashMap(m)
                    break
                }
            }
        } finally {
            lock.unlock()
        }
        val pid = readyPid ?: return false
        log("[房间] 入站连接 " + (readyMember?.get("name") ?: pid) + " <- " + key)
        try { onSocketReady?.invoke(pid, connections[pid]!!.sock!!, readyMember!!) } catch (e: Exception) {
            log("[房间] 入站回调异常: " + e.message)
        }
        return true
    }

    // ---------- 通道查询 ----------

    fun getSocket(peerId: String): DpmpSocket? {
        lock.lock()
        try {
            val conn = connections[peerId]
            if (conn != null && conn.state == C.STATE_CONNECTED && conn.sock != null) return conn.sock
        } finally { lock.unlock() }
        return null
    }

    private fun channelUsable(sock: DpmpSocket?): Boolean {
        if (sock == null) return false
        return if (sock.isUdpRtp) !sock.isClosed else true
    }

    fun markActive(peerId: String) {
        lock.lock(); try { peerLastActive[peerId] = nowSec() } finally { lock.unlock() }
    }

    private fun idleFactor(peerId: String, now: Double): Int {
        val last = peerLastActive[peerId] ?: now
        val idle = now - last
        for ((threshold, factor) in config.idleFactorTiers) {
            if (idle < threshold) return factor
        }
        return config.idleFactorMax
    }

    /** 按路径角色选路发送：优先热备，其次保守暖备，最后宽松暖备。 */
    fun getSendChannel(peerId: String, excludeSocks: Set<Int>? = null): SendChannel? {
        val exclude = excludeSocks ?: emptySet()
        val sched = pathSchedulers[peerId]
        if (sched != null) {
            for (role in listOf(C.ROLE_HOT, C.ROLE_WARM_SAFE, C.ROLE_WARM_LOOSE)) {
                val path = sched.lock.let { it.lock(); try {
                    sched.paths.values.firstOrNull { it.role == role }
                } finally { it.unlock() } } ?: continue
                if (path.proto == "tcp") {
                    lock.lock()
                    try {
                        val conn = connections[peerId]
                        if (conn != null && conn.state == C.STATE_CONNECTED && conn.sock != null &&
                            System.identityHashCode(conn.sock) !in exclude && channelUsable(conn.sock)) {
                            return SendChannel(conn.sock!!, conn.ioLock, false, role)
                        }
                    } finally { lock.unlock() }
                } else {
                    lock.lock()
                    try {
                        val rtp = udpConns[peerId]
                        if (rtp != null && System.identityHashCode(rtp) !in exclude && channelUsable(rtp)) {
                            return SendChannel(rtp, rtp.ioLock, true, role)
                        }
                    } finally { lock.unlock() }
                }
            }
        }
        lock.lock()
        try {
            val conn = connections[peerId]
            if (conn != null && conn.state == C.STATE_CONNECTED && conn.sock != null &&
                System.identityHashCode(conn.sock) !in exclude && channelUsable(conn.sock)) {
                return SendChannel(conn.sock!!, conn.ioLock, false, "?")
            }
            val rtp = udpConns[peerId]
            if (rtp != null && System.identityHashCode(rtp) !in exclude && channelUsable(rtp)) {
                return SendChannel(rtp, rtp.ioLock, true, "?")
            }
        } finally { lock.unlock() }
        return null
    }

    fun getPathRoles(peerId: String): Map<String, String> {
        val sched = pathSchedulers[peerId] ?: return emptyMap()
        sched.lock.lock(); try {
            return sched.paths.mapValues { it.value.role }
        } finally { sched.lock.unlock() }
    }

    fun hasPeer(peerId: String): Boolean {
        lock.lock(); try { return connections.containsKey(peerId) } finally { lock.unlock() }
    }

    fun getConn(peerId: String): Conn? {
        lock.lock(); try { return connections[peerId] } finally { lock.unlock() }
    }

    fun getPeerDid(peerId: String): String {
        lock.lock()
        try {
            val m = members[peerId]
            val did = m?.get("did") as? String
            if (!did.isNullOrEmpty()) return did
        } finally { lock.unlock() }
        return peerId
    }

    fun getMembers(): List<MemberInfo> {
        lock.lock()
        try {
            val out = ArrayList<MemberInfo>()
            for ((pid, m) in members) {
                val conn = connections[pid]
                var state = conn?.state ?: "idle"
                var addr: Any? = conn?.addr
                if (state != C.STATE_CONNECTED && udpConns.containsKey(pid)) {
                    state = C.STATE_CONNECTED
                    addr = "udp://" + udpConns[pid]!!.peer.first + ":" + udpConns[pid]!!.peer.second
                }
                val roles = HashMap<String, String>()
                pathSchedulers[pid]?.let { sched ->
                    sched.lock.lock(); try { sched.paths.forEach { (k, v) -> roles[k] = v.role } } finally { sched.lock.unlock() }
                }
                out.add(MemberInfo(
                    id = pid,
                    name = m["name"] as? String ?: "",
                    state = state,
                    addr = addr?.toString(),
                    pathRoles = roles
                ))
            }
            return out
        } finally { lock.unlock() }
    }

    private fun addMember(member: Map<String, Any?>) {
        val peerId = member["id"] as? String ?: return
        lock.lock(); try { members[peerId] = HashMap(member) } finally { lock.unlock() }
        try { signaling?.requestPunch(peerId) } catch (e: Exception) {
            log("[房间] 请求打洞失败: " + e.message)
        }
    }

    // ---------- 打洞任务 ----------

    private fun punchTask(peer0: Map<String, Any?>, atMs0: Long) {
        val peerId = peer0["id"] as? String ?: return
        lock.lock()
        try {
            val cnt = punchActive[peerId] ?: 0
            if (cnt >= 2) return
            punchActive[peerId] = cnt + 1
        } finally { lock.unlock() }
        punchSem.acquire()
        var atMs = atMs0
        try {
            for (attempt in 1..config.punchRetry) {
                if (!running) return
                lock.lock()
                val already = try {
                    val c = connections[peerId]
                    c != null && c.state == C.STATE_CONNECTED
                } finally { lock.unlock() }
                if (already) { log("[打洞] peer=" + peerId + " 已由其他任务连接，本任务退出"); return }
                val latest = lock.let { it.lock(); try { members[peerId] ?: peer0 } finally { it.unlock() } }
                if (attempt == 1) log("[打洞] 任务启动 peer=" + peerId + " 本地端口=" + puncher.localTcpPort)
                else log("[打洞] 第 " + attempt + " 轮重试 peer=" + peerId + "（pub_tcp=" + (latest["pub_tcp"] ?: "") + "）")
                val result = puncher.punch(latest, atMs)
                if (result != null) { installSocket(peerId, result); return }
                if (attempt < config.punchRetry) {
                    val nextRound = attempt + 1L
                    val backoff = attempt * config.punchRetryBackoff
                    val rtp = getUdpSocket(peerId)
                    val myId = signaling?.myId ?: ""
                    if (rtp != null && myId.isNotEmpty() && myId < peerId) {
                        val tGoNextI = System.currentTimeMillis() + backoff * 1000L
                        val off = rtp.getSyncOffset() ?: 0L
                        rtp.sendPunchRound(nextRound, tGoNextI + off)
                        log("[打洞] 主导轮次 " + nextRound + "，约定 T_go(本地)=" + tGoNextI)
                        sleepUntilLocal(tGoNextI)
                        atMs = 0L
                    } else if (rtp != null && myId.isNotEmpty() && myId > peerId) {
                        val tGoR = waitPunchRound(peerId, nextRound, backoff + 2.0)
                        if (tGoR != null) {
                            log("[打洞] 跟随主导轮次 " + nextRound + "，T_go(本地)=" + tGoR)
                            sleepUntilLocal(tGoR)
                        } else Thread.sleep(backoff * 1000L)
                        atMs = 0L
                    } else {
                        Thread.sleep(backoff * 1000L)
                        atMs = 0L
                    }
                }
            }
            var markFailed = false
            lock.lock()
            try {
                val conn = connections[peerId]
                if (conn != null && conn.state != C.STATE_CONNECTED) { conn.state = C.STATE_FAILED; markFailed = true }
            } finally { lock.unlock() }
            if (!markFailed) { log("[房间] 本任务失败，但 peer=" + peerId + " 已由其他任务连接，忽略"); return }
            log("[房间] 连接失败 " + (peer0["name"] ?: peerId))
            try { getUdpSocket(peerId)?.sendPunchFail() } catch (_: Exception) {}
        } finally {
            lock.lock()
            try {
                val c = (punchActive[peerId] ?: 1) - 1
                if (c <= 0) punchActive.remove(peerId) else punchActive[peerId] = c
            } finally { lock.unlock() }
            punchSem.release()
        }
    }

    private fun installSocket(peerId: String, result: PunchResult) {
        val member: Map<String, Any?>
        lock.lock()
        try {
            val old = connections[peerId]
            if (old != null && old.state == C.STATE_CONNECTED && old.sock != null) {
                try { result.channel.close() } catch (_: Exception) {}
                return
            }
            if (old != null) closeSock(old)
            val conn = Conn(members[peerId] ?: emptyMap())
            conn.state = C.STATE_CONNECTED
            conn.sock = SocketChannelAdapter(result.channel, result.ip to result.port)
            conn.addr = result.ip to result.port
            connections[peerId] = conn
            member = HashMap(members[peerId] ?: emptyMap())
        } finally { lock.unlock() }
        log("[房间] 已连接 " + (member["name"] ?: peerId) + " -> " + result.ip + ":" + result.port)
        try { getPathSched(peerId).register("tcp:" + peerId, "tcp", 0) } catch (_: Exception) {}
        try { onSocketReady?.invoke(peerId, connections[peerId]!!.sock!!, member) } catch (e: Exception) {
            log("[房间] 接收回调异常: " + e.message)
        }
        try { onStateChanged?.invoke() } catch (_: Exception) {}
    }

    private fun closeSock(conn: Conn?) {
        if (conn?.sock != null) {
            try { conn.sock!!.close() } catch (_: Exception) {}
            conn.sock = null
        }
    }

    // ---------- 保活循环 ----------

    private fun keepaliveLoop() {
        val nextDue = HashMap<String, Double>()
        while (running) {
            try { Thread.sleep((config.keepaliveTick * 1000).toLong()) } catch (_: InterruptedException) { return }
            if (!running) break
            val now = nowSec()
            var stateChanged = false

            for (pid in pathSchedulers.keys.toList()) {
                val sched = pathSchedulers[pid] ?: continue
                val items: List<Pair<String, Path>>
                sched.lock.lock(); try { items = sched.paths.map { it.key to it.value } } finally { sched.lock.unlock() }
                val idleFactor = idleFactor(pid, now)
                val liveIds = HashSet<String>()
                for ((pathId, p) in items) {
                    if (p.role == C.ROLE_DEAD) continue
                    liveIds.add(pathId)
                    if (now < (nextDue[pathId] ?: 0.0)) continue
                    val ok = probePath(pid, pathId, p)
                    if (ok) { p.scheduler.onSuccess(); p.lastSeen = now }
                    else { p.scheduler.onFailure(); log("[保活] 路径 " + pathId + " 失败（间隔=" + p.scheduler.currentInterval + "s）") }
                    nextDue[pathId] = now + p.scheduler.currentInterval * idleFactor
                }
                for (stale in nextDue.keys.filter { it !in liveIds }) nextDue.remove(stale)
            }

            val tcpItems: List<Pair<String, Conn>>
            lock.lock(); try {
                tcpItems = connections.filter { it.value.state == C.STATE_CONNECTED }.map { it.key to it.value }
            } finally { lock.unlock() }
            for ((pid, conn) in tcpItems) {
                if (!alive(conn.sock)) {
                    log("[保活] " + pid + " TCP 通道失效，重新打洞")
                    var dead = false
                    lock.lock()
                    try {
                        val cur = connections[pid]
                        if (cur === conn) { closeSock(cur); cur.state = C.STATE_FAILED; dead = true }
                    } finally { lock.unlock() }
                    if (!dead) continue
                    try { onPathFailure(pid, "tcp") } catch (_: Exception) {}
                    requestRebuild(pid)
                    stateChanged = true
                }
            }

            val udpItems: List<Pair<String, UdpReliableSocket>>
            lock.lock(); try { udpItems = udpConns.map { it.key to it.value } } finally { lock.unlock() }
            for ((pid, rtp) in udpItems) {
                try {
                    if (rtp.isDead()) { log("[保活] " + pid + " UDP-RTP 通道失联"); handleUdpPeerDead(pid); stateChanged = true }
                } catch (_: Exception) {}
            }

            if (stateChanged) try { onStateChanged?.invoke() } catch (_: Exception) {}
        }
    }

    private fun probePath(peerId: String, pathId: String, path: Path): Boolean {
        return if (path.proto == "tcp") {
            val conn = lock.let { it.lock(); try { connections[peerId] } finally { it.unlock() } }
            conn != null && conn.state == C.STATE_CONNECTED && conn.sock != null && alive(conn.sock)
        } else {
            val rtp = lock.let { it.lock(); try { udpConns[peerId] } finally { it.unlock() } }
            rtp != null && try { !rtp.isDead() } catch (_: Exception) { false }
        }
    }

    private fun alive(sock: DpmpSocket?): Boolean {
        if (sock == null) return false
        if (sock is SocketChannelAdapter) return sock.isAlive()
        if (sock is UdpReliableSocket) return !sock.isDead()
        return !sock.isClosed
    }

    companion object {
        private fun nowSec(): Double = System.nanoTime() / 1_000_000_000.0
    }
}

/** 选路结果：(sock, ioLock, isUdp, role)。 */
data class SendChannel(
    val sock: DpmpSocket,
    val ioLock: Any,
    val isUdp: Boolean,
    val role: String
)

/** 成员信息。 */
data class MemberInfo(
    val id: String,
    val name: String,
    val state: String,
    val addr: String?,
    val pathRoles: Map<String, String>
)
