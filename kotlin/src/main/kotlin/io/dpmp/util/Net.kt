package io.dpmp.util

import java.io.File
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.util.UUID

/**
 * 网络工具：本机 IP / 子网 / 广播地址 / 设备标识。
 * 与 Python 端 dpmp/util/net.py 对齐。
 */
object Net {

    /** 获取本机所有 IP。ipv6=true 则包含 IPv6。 */
    fun getAllLocalIps(ipv6: Boolean = false): List<String> {
        val ips = LinkedHashSet<String>()
        try {
            val ifaces = NetworkInterface.getNetworkInterfaces()
            while (ifaces.hasMoreElements()) {
                val nif = ifaces.nextElement()
                if (!nif.isUp) continue
                val addrs = nif.inetAddresses
                while (addrs.hasMoreElements()) {
                    val addr = addrs.nextElement()
                    if (addr.isLoopbackAddress || addr.isLinkLocalAddress) {
                        // 链路本地 IPv4 (169.254) 在 Python 端会被过滤掉的 127 类似；
                        // 这里仅保留非回环地址，链路本地 IPv4 仍保留以兼容。
                    }
                    val host = addr.hostAddress ?: continue
                    if (ipv6) {
                        if (addr is Inet6Address && !addr.isLoopbackAddress) {
                            ips.add(host.substringBefore('%'))
                        }
                    } else {
                        if (addr is Inet4Address && !addr.isLoopbackAddress) {
                            ips.add(host)
                        }
                    }
                }
            }
        } catch (_: Exception) {
        }
        if (ips.isEmpty()) {
            try {
                DatagramSocket().use { s ->
                    s.connect(InetAddress.getByName("8.8.8.8"), 1)
                    ips.add(s.localAddress.hostAddress)
                }
            } catch (_: Exception) {
            }
        }
        return ips.toList()
    }

    /** 推断某 IP 所属子网 CIDR。IPv6 暂不支持，返回 null。 */
    fun getSubnetForIp(ipStr: String): String? {
        if (ipStr.contains(":")) return null
        val parts = ipStr.split(".")
        if (parts.size != 4) return null
        val octets = parts.map { it.toIntOrNull() ?: return null }
        if (octets.any { it < 0 || it > 255 }) return null
        return when {
            ipStr.startsWith("169.254") -> ipStr + "/16"
            ipStr.startsWith("10.") -> ipStr + "/24"
            ipStr.startsWith("172.") && octets[1] in 16..31 -> ipStr + "/16"
            else -> ipStr + "/24"
        }
    }

    /** 本机所有子网 CIDR。 */
    fun getAllSubnets(): List<String> {
        val out = LinkedHashSet<String>()
        for (ip in getAllLocalIps(false)) {
            getSubnetForIp(ip)?.let { out.add(it) }
        }
        return out.toList()
    }

    /** 本机所有广播地址。 */
    fun getBroadcastAddrs(): List<String> {
        val out = LinkedHashSet<String>()
        for (ip in getAllLocalIps(false)) {
            val cidr = getSubnetForIp(ip) ?: continue
            broadcastOf(cidr)?.let { out.add(it) }
        }
        if (out.isEmpty()) out.add("255.255.255.255")
        return out.toList()
    }

    private fun broadcastOf(cidr: String): String? {
        val slash = cidr.indexOf('/')
        if (slash < 0) return null
        val ip = cidr.substring(0, slash)
        val prefix = cidr.substring(slash + 1).toIntOrNull() ?: return null
        val octets = ip.split(".").map { it.toIntOrNull() ?: return null }
        if (octets.size != 4) return null
        var addr = 0L
        for (o in octets) addr = (addr shl 8) or o.toLong()
        val mask = if (prefix == 0) 0L else (0xFFFFFFFFL shl (32 - prefix)) and 0xFFFFFFFFL
        val bcast = (addr and mask) or (mask.inv() and 0xFFFFFFFFL)
        return listOf(
            (bcast shr 24) and 0xFF,
            (bcast shr 16) and 0xFF,
            (bcast shr 8) and 0xFF,
            bcast and 0xFF
        ).joinToString(".")
    }

    /** 尽力获取本机 MAC 地址（展示用，不作识别主键）。 */
    fun getMacAddress(): String {
        try {
            val ifaces = NetworkInterface.getNetworkInterfaces()
            while (ifaces.hasMoreElements()) {
                val nif = ifaces.nextElement()
                val mac = nif.hardwareAddress ?: continue
                if (mac.size == 6) {
                    return mac.joinToString(":") { String.format("%02X", it) }
                }
            }
        } catch (_: Exception) {
        }
        return ""
    }

    /**
     * 获取或生成持久化的设备 UUID（稳定标识，跨重启不变）。
     * configPath：配置文件路径（JSON），不存在则创建。
     */
    fun getOrCreateDeviceId(configPath: String): String {
        val f = File(configPath)
        var did: String? = null
        try {
            if (f.exists()) {
                val text = f.readText(Charsets.UTF_8)
                did = parseDeviceId(text)
            }
        } catch (_: Exception) {
        }
        if (!did.isNullOrEmpty()) return did!!
        val newId = UUID.randomUUID().toString()
        try {
            f.parentFile?.mkdirs()
            f.writeText("{\"device_id\":\"" + newId + "\"}", Charsets.UTF_8)
        } catch (_: Exception) {
        }
        return newId
    }

    private fun parseDeviceId(json: String): String? {
        val key = "\"device_id\""
        val idx = json.indexOf(key)
        if (idx < 0) return null
        val colon = json.indexOf(':', idx)
        if (colon < 0) return null
        val firstQuote = json.indexOf('"', colon + 1)
        if (firstQuote < 0) return null
        val secondQuote = json.indexOf('"', firstQuote + 1)
        if (secondQuote < 0) return null
        return json.substring(firstQuote + 1, secondQuote)
    }
}
