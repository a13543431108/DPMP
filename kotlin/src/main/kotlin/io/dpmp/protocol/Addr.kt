package io.dpmp.protocol

/**
 * 地址工具：host:port 的格式化与解析（IPv4 / IPv6 双栈）。
 * 与 Python 端 dpmp/protocol/addr.py 对齐。
 */
object Addr {

    /** 格式化 "ip:port"；IPv6 用 "[ip]:port"。 */
    fun fmtHostPort(ip: String, port: Int): String {
        return if (ip.contains(":")) "[" + ip + "]:" + port else ip + ":" + port
    }

    /** 解析 "ip:port" 或 "[ipv6]:port"，返回 Pair(ip, port) 或 null。 */
    fun parseHostPort(s: String?): Pair<String, Int>? {
        if (s.isNullOrEmpty()) return null
        val t = s.trim()
        if (t.startsWith("[")) {
            val close = t.indexOf(']')
            if (close < 0) return null
            val ip = t.substring(1, close)
            val rest = t.substring(close + 1)
            if (!rest.startsWith(":")) return null
            val port = rest.substring(1).toIntOrNull() ?: return null
            return ip to port
        }
        if (!t.contains(":")) return null
        val idx = t.lastIndexOf(':')
        val ip = t.substring(0, idx)
        if (ip.contains(":")) return null  // 未加方括号的 IPv6，非法
        val port = t.substring(idx + 1).toIntOrNull() ?: return null
        return ip to port
    }
}
