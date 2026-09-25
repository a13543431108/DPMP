package io.dpmp.link

import io.dpmp.Config
import io.dpmp.protocol.C
import io.dpmp.protocol.Codec
import io.dpmp.stream.DpmpSocket
import java.io.ByteArrayOutputStream
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

/**
 * UDP-RTP：DPMP 的可靠 UDP 字节流通道（共享 socket 模式）。
 *
 * 与 Python 端 dpmp/link/rtp.py 对齐。
 */
class UdpReliableSocket(
    private val sendFn: (Pair<String, Int>, ByteArray) -> Unit,
    val peer: Pair<String, Int>,
    private val log: (String) -> Unit = {},
    private val onPeerDead: ((Pair<String, Int>) -> Unit)? = null,
    private val onSyncReady: ((Long) -> Unit)? = null,
    private val onPunchRound: ((Long, Long) -> Unit)? = null,
    private val onTcpReady: (() -> Unit)? = null,
    private val onPunchFail: (() -> Unit)? = null,
    private val config: Config = Config.DEFAULT
) : DpmpSocket {

    override val isUdpRtp: Boolean get() = true

    private val maxPayload = config.rtpMaxPayload

    @Volatile private var window = config.rtpWindowInit
    private val windowMin = config.rtpWindowMin
    private val windowMax = config.rtpWindowMax
    private val rtoMs = config.rtpRtoMs
    private val rtxInterval = config.rtpRtxInterval
    private val keepaliveInterval = config.rtpKeepaliveInterval
    private val peerDeadTimeout = config.rtpPeerDeadTimeout

    val ioLock = ReentrantLock()

    private val sendSeq = AtomicLong(0)
    private val sendQueue = LinkedHashMap<Long, Pair<ByteArray, Double>>()
    private val sendLock = ReentrantLock()
    private val sendCond = sendLock.newCondition()

    private var recvNext = 0L
    private val recvBuf = ByteArrayOutputStream()
    private val outOfOrder = HashMap<Long, ByteArray>()
    private val recvLock = ReentrantLock()
    private val recvCond = recvLock.newCondition()

    @Volatile private var closed = false
    @Volatile private var peerClosed = false
    @Volatile private var timeout: Double? = null

    @Volatile private var lastSendTime = nowSec()
    @Volatile private var lastRecvTime = nowSec()
    @Volatile private var peerDeadFired = false

    private val syncLock = ReentrantLock()
    @Volatile private var syncInProgress = false
    private val syncPending = HashMap<Long, CountDownLatch>()
    private val syncReplies = HashMap<Long, Triple<Long, Long, Long>>()
    private val syncSamples = ArrayList<Pair<Long, Long>>()
    private val syncGuard = ReentrantLock()

    @Volatile private var syncOffset: Long? = null
    @Volatile private var syncRtt: Long? = null
    @Volatile private var peerVer = 1
    private val commitLock = ReentrantLock()
    private var commitSeq = 0L
    private var lastCommitId = -1L

    init {
        Thread({ rtxLoop() }, "dpmp-rtp-rtx").apply { isDaemon = true; start() }
        Thread({ keepaliveLoop() }, "dpmp-rtp-ka").apply { isDaemon = true; start() }
    }

    // ---------- 对外接口 ----------

    override fun sendall(data: ByteArray) {
        if (data.isEmpty() || closed) return
        var offset = 0
        while (offset < data.size) {
            val chunkLen = minOf(maxPayload, data.size - offset)
            val chunk = data.copyOfRange(offset, offset + chunkLen)
            offset += chunkLen
            val pkt: ByteArray
            sendLock.lock()
            try {
                val seq = sendSeq.getAndIncrement()
                val hdr = Codec.packHeader(C.RTP_DATA, seq, recvNext, chunk.size, 0)
                pkt = hdr + chunk
                sendQueue[seq] = pkt to nowSec()
                while (sendQueue.size >= window && !closed) {
                    sendCond.await(5, TimeUnit.MILLISECONDS)
                }
            } finally {
                sendLock.unlock()
            }
            try {
                sendFn(peer, pkt)
                lastSendTime = nowSec()
            } catch (e: Exception) {
                log("[UDP-RTP] send 失败: " + e.message)
                return
            }
        }
    }

    override fun recv(n: Int): ByteArray {
        recvLock.lock()
        try {
            val dl = timeout?.let { nowSec() + it }
            while (recvBuf.size() == 0 && !peerClosed) {
                if (dl == null) {
                    recvCond.await(500, TimeUnit.MILLISECONDS)
                } else {
                    val remainMs = ((dl - nowSec()) * 1000).toLong()
                    if (remainMs <= 0) throw java.net.SocketTimeoutException("recv timeout")
                    recvCond.await(remainMs, TimeUnit.MILLISECONDS)
                }
            }
            if (recvBuf.size() == 0) return ByteArray(0)
            val all = recvBuf.toByteArray()
            val take = minOf(n, all.size)
            val data = all.copyOfRange(0, take)
            recvBuf.reset()
            if (take < all.size) recvBuf.write(all, take, all.size - take)
            return data
        } finally {
            recvLock.unlock()
        }
    }

    override fun recvExact(n: Int): ByteArray {
        val out = ByteArrayOutputStream()
        while (out.size() < n) {
            val chunk = recv(n - out.size())
            if (chunk.isEmpty()) throw java.io.IOException("UDP-RTP 对端关闭")
            out.write(chunk)
        }
        return out.toByteArray()
    }

    override fun setTimeout(t: Double?) { timeout = t }
    override fun getPeerName(): Pair<String, Int> = peer

    fun sendPunchRound(roundNo: Long, tGoR: Long): Boolean = try {
        val payload = Codec.packPunchRound(roundNo, tGoR)
        sendFn(peer, Codec.packPacket(C.RTP_PUNCH_ROUND, 0, recvNext, payload))
        lastSendTime = nowSec(); true
    } catch (e: Exception) {
        log("[打洞] 发送 PUNCH_ROUND 失败: " + e.message); false
    }

    fun sendTcpReady(): Boolean = try {
        sendFn(peer, Codec.packPacket(C.RTP_TCP_READY, 0, recvNext, ByteArray(0)))
        lastSendTime = nowSec(); true
    } catch (e: Exception) {
        log("[打洞] 发送 TCP_READY 失败: " + e.message); false
    }

    fun sendPunchFail(): Boolean = try {
        sendFn(peer, Codec.packPacket(C.RTP_PUNCH_FAIL, 0, recvNext, ByteArray(0)))
        lastSendTime = nowSec(); true
    } catch (e: Exception) {
        log("[打洞] 发送 PUNCH_FAIL 失败: " + e.message); false
    }

    fun getSyncOffset(): Long? = syncOffset
    fun getSyncRtt(): Long? = syncRtt
    fun getPeerVer(): Int = peerVer

    /** 发起 SYNC 采样，完成后发送 SYNC_COMMIT 约定 TCP 打洞时刻。阻塞至采样完成。 */
    fun startSync() {
        syncGuard.lock()
        try {
            if (syncInProgress) { log("[SYNC] 已有采样在进行，跳过"); return }
            syncInProgress = true
            syncSamples.clear(); syncPending.clear(); syncReplies.clear()
        } finally {
            syncGuard.unlock()
        }
        try {
            for (i in 0 until config.syncSampleCount) {
                if (closed) return
                val t1 = nowMs()
                val latch = CountDownLatch(1)
                syncGuard.lock(); try { syncPending[t1] = latch } finally { syncGuard.unlock() }
                try {
                    val payload = Codec.packSyncReq(t1, C.RTP_PROTO_VER)
                    sendFn(peer, Codec.packPacket(C.RTP_SYNC_REQ, 0, recvNext, payload))
                    lastSendTime = nowSec()
                } catch (e: Exception) {
                    log("[SYNC] 发送 REQ 失败: " + e.message); break
                }
                val ok = latch.await((config.syncTimeout * 1000).toLong(), TimeUnit.MILLISECONDS)
                val reply = syncGuard.let { g ->
                    g.lock(); try { syncPending.remove(t1); syncReplies.remove(t1) } finally { g.unlock() }
                }
                if (ok && reply != null) {
                    syncSamples.add(reply.first to reply.second)
                    log("[SYNC] 采样 " + (i + 1) + "/" + config.syncSampleCount +
                        ": RTT=" + reply.first + "ms offset=" + reply.second + "ms")
                } else {
                    log("[SYNC] 采样 " + (i + 1) + " 超时")
                }
                Thread.sleep(50)
            }
            if (syncSamples.isEmpty()) { log("[SYNC] 无有效采样，放弃 SYNC"); return }
            val best = syncSamples.minByOrNull { it.first }!!
            syncOffset = best.second
            syncRtt = best.first
            log("[SYNC] 最佳：RTT=" + best.first + "ms offset=" + best.second +
                "ms（共 " + syncSamples.size + " 次采样，peer_ver=" + peerVer + "）")

            val deltaMs = config.syncCommitDelayMs
            val tGo = nowMs() + deltaMs
            val bestOffset = syncOffset ?: 0L
            commitSeq += 1
            val commitId = commitSeq
            try {
                if (peerVer >= C.RTP_PROTO_VER) {
                    val tGoR = tGo + bestOffset
                    val payload = Codec.packSyncCommitV2(tGoR, commitId)
                    for (r in 0 until config.syncCommitResend) {
                        sendFn(peer, Codec.packPacket(C.RTP_SYNC_COMMIT, 0, recvNext, payload))
                        lastSendTime = nowSec()
                        Thread.sleep((config.syncCommitResendInterval * 1000).toLong())
                    }
                    log("[SYNC] 已发 COMMIT(v2) T_go_I=" + tGo + " T_go_R=" + tGoR +
                        " id=" + commitId + " x" + config.syncCommitResend)
                } else {
                    val payload = Codec.packSyncCommitV1(deltaMs)
                    sendFn(peer, Codec.packPacket(C.RTP_SYNC_COMMIT, 0, recvNext, payload))
                    lastSendTime = nowSec()
                    log("[SYNC] 已发 COMMIT(v1) T_go=" + tGo + "（" + deltaMs + "ms 后）")
                }
            } catch (e: Exception) {
                log("[SYNC] 发送 COMMIT 失败: " + e.message); return
            }
            onSyncReady?.invoke(tGo)
        } finally {
            syncGuard.lock(); try { syncInProgress = false } finally { syncGuard.unlock() }
        }
    }

    fun isDead(): Boolean {
        if (closed) return true
        if (peerDeadFired) return true
        return (nowSec() - lastRecvTime) > peerDeadTimeout
    }

    fun getTimeout(): Double? = timeout
    override val isClosed: Boolean get() = closed

    override fun close() {
        if (closed) return
        closed = true
        try { sendFn(peer, Codec.packPacket(C.RTP_FIN, 0, recvNext, ByteArray(0))) } catch (_: Exception) {}
        recvLock.lock(); try { recvCond.signalAll() } finally { recvLock.unlock() }
        sendLock.lock(); try { sendCond.signalAll() } finally { sendLock.unlock() }
    }

    // ---------- 接收（由全局接收循环调用） ----------

    fun onPacket(data: ByteArray) {
        if (data.size < C.RTP_HEADER_SIZE || closed) return
        val hdr = Codec.unpackHeader(data) ?: return
        val payload = data.copyOfRange(C.RTP_HEADER_SIZE, minOf(C.RTP_HEADER_SIZE + hdr.length, data.size))
        lastRecvTime = nowSec()
        when (hdr.type) {
            C.RTP_DATA -> onData(hdr.seq, hdr.ack, payload)
            C.RTP_ACK -> onAck(hdr.ack)
            C.RTP_FIN -> {
                peerClosed = true
                recvLock.lock(); try { recvCond.signalAll() } finally { recvLock.unlock() }
            }
            C.RTP_KEEPALIVE -> {}
            C.RTP_SYNC_REQ -> onSyncReq(payload)
            C.RTP_SYNC_ACK -> onSyncAck(payload)
            C.RTP_SYNC_COMMIT -> onSyncCommit(payload)
            C.RTP_PUNCH_ROUND -> onPunchRoundPkt(payload)
            C.RTP_TCP_READY -> try { onTcpReady?.invoke() } catch (e: Exception) { log("[打洞] on_tcp_ready 异常: " + e.message) }
            C.RTP_PUNCH_FAIL -> try { onPunchFail?.invoke() } catch (e: Exception) { log("[打洞] on_punch_fail 异常: " + e.message) }
        }
    }

    private fun onData(seq: Long, ack: Long, payload: ByteArray) {
        onAck(ack)
        try {
            sendFn(peer, Codec.packHeader(C.RTP_ACK, 0, seq, 0, 0))
            lastSendTime = nowSec()
        } catch (_: Exception) {}
        recvLock.lock()
        try {
            if (seq == recvNext) {
                recvBuf.write(payload)
                recvNext += 1
                while (outOfOrder.containsKey(recvNext)) {
                    recvBuf.write(outOfOrder.remove(recvNext)!!)
                    recvNext += 1
                }
                recvCond.signalAll()
            } else if (seq > recvNext) {
                outOfOrder[seq] = payload
            }
        } finally {
            recvLock.unlock()
        }
    }

    private fun onAck(ack: Long) {
        sendLock.lock()
        try {
            val done = sendQueue.keys.filter { it < ack }
            val advanced = done.size
            for (s in done) sendQueue.remove(s)
            if (advanced > 0) window = minOf(windowMax, window + advanced)
            sendCond.signalAll()
        } finally {
            sendLock.unlock()
        }
    }

    private fun onSyncReq(payload: ByteArray) {
        val (t1, ver) = try { Codec.unpackSyncReq(payload) } catch (e: Exception) { return }
        if (ver > peerVer) peerVer = ver
        val t2 = nowMs(); val t3 = t2
        try {
            val ackPayload = Codec.packSyncAck(t1, t2, t3, C.RTP_PROTO_VER)
            sendFn(peer, Codec.packPacket(C.RTP_SYNC_ACK, 0, recvNext, ackPayload))
            lastSendTime = nowSec()
        } catch (e: Exception) {
            log("[SYNC] 发送 ACK 失败: " + e.message)
        }
    }

    private fun onSyncAck(payload: ByteArray) {
        val arr = try { Codec.unpackSyncAck(payload) } catch (e: Exception) { return }
        val t1 = arr[0]; val t2 = arr[1]; val t3 = arr[2]; val ver = arr[3].toInt()
        if (ver > peerVer) peerVer = ver
        val t4 = nowMs()
        val rtt = (t4 - t1) - (t3 - t2)
        val offset = ((t2 - t1) + (t3 - t4)) / 2
        syncGuard.lock()
        try {
            val latch = syncPending[t1]
            if (latch != null) {
                syncReplies[t1] = Triple(rtt, offset, t4)
                latch.countDown()
            }
        } finally {
            syncGuard.unlock()
        }
    }

    private fun onSyncCommit(payload: ByteArray) {
        var tGoLocal: Long? = null
        try {
            if (payload.size == 16) {
                val (tGoR, commitId) = Codec.unpackSyncCommitV2(payload)
                commitLock.lock()
                try {
                    if (commitId <= lastCommitId) return
                    lastCommitId = commitId
                } finally {
                    commitLock.unlock()
                }
                val offset = syncOffset ?: 0L
                tGoLocal = tGoR - offset
            } else if (payload.size == 8) {
                val deltaMs = Codec.unpackSyncCommitV1(payload)
                tGoLocal = nowMs() + deltaMs
            }
        } catch (e: Exception) {
            return
        }
        if (tGoLocal != null) {
            try { onSyncReady?.invoke(tGoLocal!!) } catch (e: Exception) {
                log("[SYNC] on_sync_ready(响应方) 异常: " + e.message)
            }
        }
    }

    private fun onPunchRoundPkt(payload: ByteArray) {
        val (roundNo, tGoR) = try { Codec.unpackPunchRound(payload) } catch (e: Exception) { return }
        try { onPunchRound?.invoke(roundNo, tGoR) } catch (e: Exception) {
            log("[打洞] on_punch_round 异常: " + e.message)
        }
    }

    // ---------- 后台线程 ----------

    private fun rtxLoop() {
        while (!closed) {
            try { Thread.sleep((rtxInterval * 1000).toLong()) } catch (_: InterruptedException) { return }
            val now = nowSec()
            val resend = ArrayList<ByteArray>()
            sendLock.lock()
            try {
                for ((seq, v) in sendQueue.entries.toList()) {
                    if (now - v.second > rtoMs / 1000.0) {
                        sendQueue[seq] = v.first to now
                        resend.add(v.first)
                    }
                }
                if (resend.isNotEmpty()) window = maxOf(windowMin, window / 2)
            } finally {
                sendLock.unlock()
            }
            for (pkt in resend) {
                try { sendFn(peer, pkt); lastSendTime = nowSec() } catch (_: Exception) {}
            }
        }
    }

    private fun keepaliveLoop() {
        while (!closed) {
            try { Thread.sleep(1000) } catch (_: InterruptedException) { return }
            val now = nowSec()
            if (now - lastSendTime >= keepaliveInterval) {
                try {
                    sendFn(peer, Codec.packHeader(C.RTP_KEEPALIVE, 0, recvNext, 0, 0))
                    lastSendTime = now
                } catch (_: Exception) {}
            }
            if (!peerDeadFired && now - lastRecvTime > peerDeadTimeout) {
                peerDeadFired = true
                try { onPeerDead?.invoke(peer) } catch (_: Exception) {}
            }
        }
    }

    companion object {
        private fun nowSec(): Double = System.nanoTime() / 1_000_000_000.0
        private fun nowMs(): Long = System.currentTimeMillis()
    }
}
