package io.dpmp

import io.dpmp.protocol.C

/**
 * DPMP 应用默认值（非协议常量）。
 *
 * 协议常量见 io.dpmp.protocol.C。本对象放**部署相关的默认值**——
 * 默认信令服务器等，方便开箱即用。这些值随时可被使用者的参数覆盖。
 *
 * ⚠️ 默认服务器是「便利」，不是「依赖」。
 * 与 Python 端 dpmp/defaults.py 对齐。
 */
object Defaults {
    const val DEFAULT_SERVER: String = "42.194.133.132"
    const val DEFAULT_SERVER_PORT: Int = C.DEFAULT_SERVER_PORT
    const val DEFAULT_SERVER_TCP_PORT: Int = C.DEFAULT_SERVER_TCP_PORT
    const val DEFAULT_NAT_PROBE_PORT: Int = C.NAT_PROBE_PORT

    /** 默认服务器候选。每项为 (ip, 信令端口, TCP 映射观测端口, NAT 探测端口)。 */
    fun defaultServers(): List<ServerSpec> =
        listOf(ServerSpec(DEFAULT_SERVER, DEFAULT_SERVER_PORT, DEFAULT_SERVER_TCP_PORT, DEFAULT_NAT_PROBE_PORT))
}

/** 一个信令服务器描述：(ip, 信令端口, TCP 映射观测端口, NAT 探测端口)。 */
data class ServerSpec(
    val ip: String,
    val port: Int,
    val tcpPort: Int = C.DEFAULT_SERVER_TCP_PORT,
    val natPort: Int = C.NAT_PROBE_PORT
)
