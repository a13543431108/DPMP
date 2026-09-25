package io.dpmp

/**
 * DPMP — Dual-Punch Multi-Path Protocol（Kotlin 端）。
 *
 * P2P 连接与传输底层库。与 Python 端 dpmp 协议完全兼容。
 *
 * 对外分层：
 *   · io.dpmp.protocol   协议常量与编解码
 *   · io.dpmp.link       连接层（发现 / 信令 / 双打洞 / UDP-RTP / 路径调度 / 连接管理）
 *   · io.dpmp.stream     传输层（统一字节流通道 + 多径选路）
 *   · io.dpmp.util       工具层（网络探测 / 设备标识）
 */
object Dpmp {
    const val VERSION: String = "0.1.3"
    const val PROTOCOL: String = "DPMP/1.0"
}
