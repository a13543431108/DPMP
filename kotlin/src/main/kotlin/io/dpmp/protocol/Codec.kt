package io.dpmp.protocol

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * DPMP 二进制编解码。
 *
 * UDP-RTP 包头（大端）：
 *     [1B type][4B seq][4B ack][2B length][1B reserved] = 12 字节
 *
 * 与 Python 端 dpmp/protocol/codec.py 逐字节一致。
 */
object Codec {

    /** 12 字节包头：!B I I H B */
    const val HEADER_SIZE = C.RTP_HEADER_SIZE

    /** 解析后的包头。 */
    data class Header(
        val type: Int,
        val seq: Long,
        val ack: Long,
        val length: Int,
        val reserved: Int
    )

    // ---------- 包头 ----------

    /** 打包 12 字节 UDP-RTP 包头。 */
    fun packHeader(
        ptype: Int, seq: Long = 0L, ack: Long = 0L,
        length: Int = 0, reserved: Int = 0
    ): ByteArray {
        val buf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN)
        buf.put((ptype and 0xFF).toByte())
        buf.putInt((seq and 0xFFFFFFFFL).toInt())
        buf.putInt((ack and 0xFFFFFFFFL).toInt())
        buf.putShort((length and 0xFFFF).toShort())
        buf.put((reserved and 0xFF).toByte())
        return buf.array()
    }

    /** 解析 12 字节包头；不足 12 字节返回 null。 */
    fun unpackHeader(data: ByteArray): Header? {
        if (data.size < HEADER_SIZE) return null
        val buf = ByteBuffer.wrap(data, 0, HEADER_SIZE).order(ByteOrder.BIG_ENDIAN)
        val type = buf.get().toInt() and 0xFF
        val seq = buf.int.toLong() and 0xFFFFFFFFL
        val ack = buf.int.toLong() and 0xFFFFFFFFL
        val length = buf.short.toInt() and 0xFFFF
        val reserved = buf.get().toInt() and 0xFF
        return Header(type, seq, ack, length, reserved)
    }

    /** 打包一个完整报文（头 + payload）。 */
    fun packPacket(ptype: Int, seq: Long, ack: Long, payload: ByteArray = ByteArray(0)): ByteArray {
        val hdr = packHeader(ptype, seq, ack, payload.size, 0)
        val out = ByteArray(hdr.size + payload.size)
        System.arraycopy(hdr, 0, out, 0, hdr.size)
        System.arraycopy(payload, 0, out, hdr.size, payload.size)
        return out
    }

    // ---------- SYNC_REQ ----------
    // !QB : t1(ms), proto_ver

    fun packSyncReq(t1Ms: Long, protoVer: Int = C.RTP_PROTO_VER): ByteArray {
        val buf = ByteBuffer.allocate(9).order(ByteOrder.BIG_ENDIAN)
        buf.putLong(t1Ms)
        buf.put((protoVer and 0xFF).toByte())
        return buf.array()
    }

    /** 返回 [t1Ms, protoVer]。 */
    fun unpackSyncReq(payload: ByteArray): Pair<Long, Int> {
        val buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN)
        val t1 = buf.long
        val ver = buf.get().toInt() and 0xFF
        return t1 to ver
    }

    // ---------- SYNC_ACK ----------
    // !QQQB : t1, t2, t3, proto_ver

    fun packSyncAck(t1: Long, t2: Long, t3: Long, protoVer: Int = C.RTP_PROTO_VER): ByteArray {
        val buf = ByteBuffer.allocate(25).order(ByteOrder.BIG_ENDIAN)
        buf.putLong(t1); buf.putLong(t2); buf.putLong(t3)
        buf.put((protoVer and 0xFF).toByte())
        return buf.array()
    }

    /** 返回 [t1, t2, t3, protoVer]。 */
    fun unpackSyncAck(payload: ByteArray): LongArray {
        val buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN)
        val t1 = buf.long; val t2 = buf.long; val t3 = buf.long
        val ver = (buf.get().toInt() and 0xFF).toLong()
        return longArrayOf(t1, t2, t3, ver)
    }

    // ---------- SYNC_COMMIT ----------
    // v2 !QQ : t_go_r, commit_id
    // v1 !Q  : delta_ms

    fun packSyncCommitV2(tGoR: Long, commitId: Long): ByteArray {
        val buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
        buf.putLong(tGoR); buf.putLong(commitId)
        return buf.array()
    }

    fun unpackSyncCommitV2(payload: ByteArray): Pair<Long, Long> {
        val buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN)
        return buf.long to buf.long
    }

    fun packSyncCommitV1(deltaMs: Long): ByteArray {
        val buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
        buf.putLong(deltaMs)
        return buf.array()
    }

    fun unpackSyncCommitV1(payload: ByteArray): Long {
        return ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).long
    }

    // ---------- PUNCH_ROUND ----------
    // !QQ : round_no, t_go_r

    fun packPunchRound(roundNo: Long, tGoR: Long): ByteArray {
        val buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
        buf.putLong(roundNo); buf.putLong(tGoR)
        return buf.array()
    }

    fun unpackPunchRound(payload: ByteArray): Pair<Long, Long> {
        val buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN)
        return buf.long to buf.long
    }
}
