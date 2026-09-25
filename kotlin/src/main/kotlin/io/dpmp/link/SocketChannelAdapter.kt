package io.dpmp.link

import io.dpmp.stream.DpmpSocket
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel

/**
 * 把 Java NIO SocketChannel 适配成统一的 DpmpSocket 接口（TCP 通道）。
 *
 * 打洞用非阻塞 connect + Selector，成功后切回阻塞模式，交由本适配器
 * 提供流式读写。
 */
class SocketChannelAdapter(
    private val channel: SocketChannel,
    private val peer: Pair<String, Int>
) : DpmpSocket {

    override val isUdpRtp: Boolean get() = false
    override val isClosed: Boolean get() = !channel.isOpen

    private val input: InputStream by lazy {
        if (!channel.isBlocking) channel.configureBlocking(true)
        channel.socket().getInputStream()
    }
    private val output: OutputStream by lazy {
        if (!channel.isBlocking) channel.configureBlocking(true)
        channel.socket().getOutputStream()
    }

    @Volatile private var timeoutMs: Int = 0

    init {
        try { channel.socket().soTimeout = timeoutMs } catch (_: Exception) {}
    }

    override fun sendall(data: ByteArray) {
        output.write(data)
        output.flush()
    }

    override fun recv(n: Int): ByteArray {
        val buf = ByteArray(n)
        val read = input.read(buf)
        if (read <= 0) return ByteArray(0)
        return buf.copyOfRange(0, read)
    }

    override fun recvExact(n: Int): ByteArray {
        val out = ByteArrayOutputStream()
        while (out.size() < n) {
            val chunk = recv(n - out.size())
            if (chunk.isEmpty()) throw java.io.IOException("TCP 对端关闭")
            out.write(chunk)
        }
        return out.toByteArray()
    }

    override fun setTimeout(t: Double?) {
        timeoutMs = if (t == null) 0 else (t * 1000).toInt()
        try { channel.socket().soTimeout = timeoutMs } catch (_: Exception) {}
    }

    override fun getPeerName(): Pair<String, Int> = peer

    override fun close() {
        try { channel.close() } catch (_: Exception) {}
    }

    /** 探活：非阻塞 peek 一字节，无数据且未断开视为存活。 */
    fun isAlive(): Boolean {
        if (!channel.isOpen) return false
        return try {
            val sock = channel.socket()
            val oldTimeout = sock.soTimeout
            sock.soTimeout = 1
            try {
                val in1 = sock.getInputStream()
                // 无数据可读会抛 SocketTimeoutException，视为存活
                val b = in1.read()
                b >= 0  // 读到数据或 -1（对端关闭）
            } catch (e: java.net.SocketTimeoutException) {
                true
            } catch (_: Exception) {
                false
            } finally {
                try { sock.soTimeout = oldTimeout } catch (_: Exception) {}
            }
        } catch (_: Exception) {
            false
        }
    }
}
