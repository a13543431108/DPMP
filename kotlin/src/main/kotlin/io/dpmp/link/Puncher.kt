package io.dpmp.link

import io.dpmp.Config
import io.dpmp.protocol.Addr
import java.net.InetSocketAddress
import java.net.StandardSocketOptions
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

/** 打洞成功的结果：一条保持打开的已连接 socket。 */
class PunchResult(val ip: String, val port: Int, val channel: SocketChannel)

/**
 * 对单个对端执行 TCP 打洞（TCP 同时打开）。
 *
 * 与 Python 端 dpmp/link/puncher.py 对齐。
 */
class HolePuncher(
    val localTcpPort: Int,
    private val log: (String) -> Unit = {},
    clockOffsetMs: Long = 0L,
    private val config: Config = Config.DEFAULT
) {
    // 双打洞：UDP 打洞成功后记录的对方 UDP 公网端口，用于预测 TCP 端口
    private val udpHint = HashMap<String, Pair<String, Int>>()
    private val hintLock = ReentrantLock()

    @Volatile var connectTimeout: Double = config.punchConnectTimeout

    @Volatile private var clockOffsetMs: Long = clockOffsetMs
    @Volatile var verbose: Boolean = false

    fun setClockOffset(offsetMs: Long) { clockOffsetMs = offsetMs }

    /** 按 SYNC RTT 调整 connect 超时（3~8 秒）。 */
    fun setAdaptiveTimeout(rttMs: Long) {
        if (rttMs <= 0) return
        val t = maxOf(3.0, minOf(8.0, rttMs / 1000.0 * 4.0))
        connectTimeout = t
    }

    fun setUdpHint(peerId: String, ip: String, port: Int) {
        hintLock.lock(); try { udpHint[peerId] = ip to port } finally { hintLock.unlock() }
    }

    /** 执行打洞。peer 为 {id, tcp, lan:[...], pub_tcp:"ip:port"}。 */
    fun punch(peer: Map<String, Any?>, atMs: Long): PunchResult? {
        val tcpPort = (peer["tcp"] as? Number)?.toInt() ?: 0
        if (tcpPort == 0) {
            log("[打洞] 放弃：对方 tcp 端口为 0 (peer=" + peer["id"] + ")")
            return null
        }
        val candidates = buildCandidates(peer)
        if (candidates.isEmpty()) {
            log("[打洞] 放弃：候选地址为空 (peer=" + peer["id"] + ")")
            return null
        }
        if (verbose) {
            log("[打洞] peer=" + peer["id"] + " 候选=" +
                candidates.joinToString(", ") { it.first + ":" + it.second })
        }
        waitUntil(atMs)
        val result = connectCandidatesParallel(candidates, peer["id"] as? String ?: "")
        if (result != null) {
            log("[打洞] 成功 -> " + result.ip + ":" + result.port + " (peer=" + peer["id"] + ")")
            return result
        }
        if (verbose) log("[打洞] 失败 peer=" + peer["id"] + "（所有候选均不可达）")
        return null
    }

    private fun connectCandidatesParallel(
        candidates: List<Pair<String, Int>>,
        peerId: String
    ): PunchResult? {
        if (candidates.size == 1) return tryConnect(candidates[0].first, candidates[0].second)
        val n = candidates.size
        val barrier = CountDownLatch(n)
        val winRef = java.util.concurrent.atomic.AtomicReference<PunchResult?>(null)
        val done = CountDownLatch(1)
        val winLock = ReentrantLock()
        val sem = Semaphore(config.punchCandidateConcurrency)

        val threads = ArrayList<Thread>()
        for ((ip, port) in candidates) {
            val t = Thread({
                try { barrier.countDown(); barrier.await(3, TimeUnit.SECONDS) } catch (_: Exception) {}
                if (done.count == 0L) return@Thread
                sem.acquire()
                val r = try {
                    if (done.count == 0L) null else tryConnect(ip, port)
                } finally {
                    sem.release()
                }
                if (r == null) return@Thread
                winLock.lock()
                try {
                    if (done.count == 0L) {
                        winRef.set(r)
                        done.countDown()
                    } else {
                        try { r.channel.close() } catch (_: Exception) {}
                    }
                } finally {
                    winLock.unlock()
                }
            }, "dpmp-punch-cand")
            t.isDaemon = true
            t.start()
            threads.add(t)
        }
        done.await((connectTimeout + 2.0).toLong(), TimeUnit.SECONDS)
        for (t in threads) t.join(200)
        return winRef.get()
    }

    private fun buildCandidates(peer: Map<String, Any?>): List<Pair<String, Int>> {
        val tcpPort = (peer["tcp"] as? Number)?.toInt() ?: 0
        val result = ArrayList<Pair<String, Int>>()
        val seen = HashSet<Pair<String, Int>>()
        @Suppress("UNCHECKED_CAST")
        val lan = peer["lan"] as? List<String> ?: emptyList()
        for (ip in lan) {
            if (ip.isNotEmpty() && seen.add(ip to tcpPort)) result.add(ip to tcpPort)
        }
        val pubTcp = peer["pub_tcp"] as? String ?: ""
        val parsed = Addr.parseHostPort(pubTcp)
        if (parsed != null) {
            if (seen.add(parsed)) result.add(parsed)
        }
        // 双打洞端口预测：pub_tcp 为空时，用 UDP 打洞的公网端口 ±2 预测
        if (parsed == null) {
            val hint = hintLock.let { it.lock(); try { udpHint[peer["id"] as? String] } finally { it.unlock() } }
            if (hint != null && !hint.first.contains(":")) {
                val (hip, hport) = hint
                for (dp in config.punchPortPredictRange) {
                    val pp = hport + dp
                    if (pp <= 0 || pp > 65535) continue
                    if (seen.add(hip to pp)) result.add(hip to pp)
                }
                if (verbose) log("[打洞] 追加 UDP 预测候选 " + hip + ":" + hport + "±2 (peer=" + peer["id"] + ")")
            }
        }
        return result
    }

    private fun waitUntil(atMs: Long) {
        if (atMs == 0L) return
        val localTarget = (atMs - clockOffsetMs) / 1000.0
        val remain = localTarget - System.currentTimeMillis() / 1000.0
        if (remain > 0) Thread.sleep(minOf(remain, 2.0).toLong().coerceAtLeast(1))
    }

    private fun tryConnect(ip: String, port: Int): PunchResult? {
        var channel: SocketChannel? = null
        var bindOk = false
        try {
            channel = SocketChannel.open()
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true)
            try {
                channel.bind(InetSocketAddress(ipToBindAddr(ip), localTcpPort))
                bindOk = true
            } catch (be: Exception) {
                log("[打洞] bind " + localTcpPort + " 失败: " + be.message + "（继续，走内核随机端口）")
            }
            channel.configureBlocking(false)
            val t0 = System.currentTimeMillis()
            val rc = channel.connect(InetSocketAddress(ip, port))
            if (rc) {
                channel.configureBlocking(true)
                log("[打洞] connect 立即成功 " + ip + ":" + port + "（bind=" + bindOk + "）")
                return PunchResult(ip, port, channel)
            }
            val selector = Selector.open()
            channel.register(selector, SelectionKey.OP_CONNECT)
            val ready = selector.select((connectTimeout * 1000).toLong())
            val dtMs = (System.currentTimeMillis() - t0).toInt()
            if (ready == 0) {
                if (verbose) log("[打洞] connect 超时 " + ip + ":" + port + "（" + dtMs + "ms, bind=" + bindOk + "）")
            } else {
                try {
                    if (channel.finishConnect()) {
                        channel.configureBlocking(true)
                        log("[打洞] connect 成功 " + ip + ":" + port + "（" + dtMs + "ms, bind=" + bindOk + "）")
                        selector.close()
                        return PunchResult(ip, port, channel)
                    }
                } catch (_: Exception) {
                }
                if (verbose) log("[打洞] connect 失败 " + ip + ":" + port + "（" + dtMs + "ms, bind=" + bindOk + "）")
            }
            selector.close()
        } catch (e: Exception) {
            if (verbose) log("[打洞] connect 异常 " + ip + ":" + port + " -> " + e.message)
        }
        try { channel?.close() } catch (_: Exception) {}
        return null
    }

    private fun ipToBindAddr(ip: String): String = if (ip.contains(":")) "::" else "0.0.0.0"
}
