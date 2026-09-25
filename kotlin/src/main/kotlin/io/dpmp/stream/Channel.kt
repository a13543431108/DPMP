package io.dpmp.stream

/**
 * 统一通道抽象：屏蔽 TCP socket 与 UDP-RTP 的差异。
 *
 * 对上提供 sendall / recv / recvExact / close / setTimeout，
 * 让业务层无需区分底层协议。
 *
 * 与 Python 端 dpmp/stream/channel.py 对齐。
 */
interface DpmpSocket {
    fun sendall(data: ByteArray)
    fun recv(n: Int): ByteArray
    fun recvExact(n: Int): ByteArray
    fun setTimeout(t: Double?)
    fun getPeerName(): Pair<String, Int>
    fun close()

    /** 是否 UDP-RTP 通道。 */
    val isUdpRtp: Boolean
        get() = false

    /** 是否已关闭。 */
    val isClosed: Boolean
        get() = false
}

/** 判断一个 socket 是否为 UDP-RTP 通道。 */
fun isUdpRtp(sock: Any?): Boolean = sock is DpmpSocket && sock.isUdpRtp

/**
 * 统一通道。
 *
 * @param sock   底层 socket（TCP socket 或 UdpReliableSocket 包装）
 * @param ioLock 收发串行化锁
 * @param role   当前路径角色（hot / warm_safe / warm_loose / "?"）
 */
class Channel(
    val sock: DpmpSocket,
    val ioLock: Any,
    val role: String = "?"
) {
    val isUdp: Boolean = sock.isUdpRtp

    fun sendall(data: ByteArray) = sock.sendall(data)
    fun recv(n: Int): ByteArray = sock.recv(n)
    fun recvExact(n: Int): ByteArray = sock.recvExact(n)
    fun setTimeout(t: Double?) = sock.setTimeout(t)
    fun getPeerName(): Pair<String, Int> = sock.getPeerName()
    fun close() = sock.close()
}

/** 把底层 socket 包装成统一 Channel。 */
fun wrapChannel(sock: DpmpSocket, ioLock: Any, role: String = "?"): Channel =
    Channel(sock, ioLock, role)
