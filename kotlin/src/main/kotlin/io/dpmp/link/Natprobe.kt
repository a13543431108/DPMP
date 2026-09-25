package io.dpmp.link

import io.dpmp.protocol.C
import io.dpmp.protocol.Json
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress

/**
 * NAT 类型探测。
 *
 * 原理：向服务器两个不同的 UDP 端口发探测包，对比服务器观测到的公网映射。
 *   · 锥形 NAT（Cone）：同一本地端口 -> 任意目标的映射相同
 *   · 对称 NAT（Symmetric）：不同目标的映射不同
 *
 * 与 Python 端 dpmp/link/natprobe.py 对齐。
 */
object Natprobe {

    data class Result(val type: String, val primary: String?, val alt: String?)

    fun detectNatType(
        serverIp: String,
        serverPort: Int = C.DEFAULT_SERVER_PORT,
        probePort: Int = C.NAT_PROBE_PORT,
        timeout: Double = 1.5,
        log: (String) -> Unit = {}
    ): Result {
        val socket: DatagramSocket
        try {
            socket = DatagramSocket()
            socket.soTimeout = (timeout * 1000).toInt()
        } catch (e: Exception) {
            log("[NAT探测] 创建 UDP socket 失败: " + e.message)
            return Result("unknown", null, null)
        }
        var primary: String? = null
        var alt: String? = null
        try {
            fun probe(targetPort: Int, tag: String): Boolean {
                return try {
                    val payload = Json.encode(mapOf(
                        "type" to C.T_NAT_PROBE,
                        "ver" to C.DPMP_VER,
                        "probe" to tag
                    )).toByteArray(Charsets.UTF_8)
                    socket.send(DatagramPacket(payload, payload.size,
                        InetAddress.getByName(serverIp), targetPort))
                    true
                } catch (e: Exception) {
                    log("[NAT探测] 向 " + serverIp + ":" + targetPort + " 发包失败: " + e.message)
                    false
                }
            }

            if (probe(serverPort, "primary")) {
                try {
                    val buf = ByteArray(4096)
                    val pkt = DatagramPacket(buf, buf.size)
                    socket.receive(pkt)
                    val msg = Json.decode(String(pkt.data, 0, pkt.length, Charsets.UTF_8))
                    if (msg["type"] == C.T_NAT_PROBE_REPLY) primary = msg["pub"] as? String
                } catch (_: Exception) {}
            }
            if (probe(probePort, "alt")) {
                try {
                    val buf = ByteArray(4096)
                    val pkt = DatagramPacket(buf, buf.size)
                    socket.receive(pkt)
                    val msg = Json.decode(String(pkt.data, 0, pkt.length, Charsets.UTF_8))
                    if (msg["type"] == C.T_NAT_PROBE_REPLY) alt = msg["pub"] as? String
                } catch (_: Exception) {}
            }
        } catch (e: Exception) {
            log("[NAT探测] 探测异常: " + e.message)
        } finally {
            try { socket.close() } catch (_: Exception) {}
        }

        return when {
            primary != null && alt != null ->
                if (primary == alt) Result("cone", primary, alt) else Result("symmetric", primary, alt)
            primary != null || alt != null -> Result("unknown", primary, alt)
            else -> Result("no_udp", null, null)
        }
    }
}
