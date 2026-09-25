package io.dpmp

import io.dpmp.protocol.C
import io.dpmp.protocol.Codec
import io.dpmp.protocol.Json
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 协议编解码单测：加载仓库根目录的 test_vectors.json，
 * 断言 Kotlin 端编解码与 Python 端逐字节一致（防两端漂移）。
 */
class ProtocolVectorsTest {

    private fun loadVectors(): Map<String, Any?> {
        // 从测试运行目录向上找到 test_vectors.json
        var dir: File? = File(".").absoluteFile
        while (dir != null) {
            val f = File(dir, "test_vectors.json")
            if (f.exists()) return Json.decode(f.readText(Charsets.UTF_8))
            val f2 = File(dir, ".." + File.separator + "test_vectors.json")
            if (f2.exists()) return Json.decode(f2.readText(Charsets.UTF_8))
            dir = dir.parentFile
        }
        // 退化：直接尝试仓库根
        val root = File("../test_vectors.json")
        if (root.exists()) return Json.decode(root.readText(Charsets.UTF_8))
        error("找不到 test_vectors.json")
    }

    @Test
    fun rtpHeaderSample() {
        val tv = loadVectors()
        @Suppress("UNCHECKED_CAST")
        val sample = (tv["rtp_header"] as Map<String, Any?>)["sample"] as Map<String, Any?>
        val type = (sample["type"] as Number).toInt()
        val seq = (sample["seq"] as Number).toLong()
        val ack = (sample["ack"] as Number).toLong()
        val length = (sample["length"] as Number).toInt()
        val reserved = (sample["reserved"] as Number).toInt()
        val hex = sample["hex"] as String

        val pkt = Codec.packHeader(type, seq, ack, length, reserved)
        assertEquals(hex, pkt.joinToString("") { "%02x".format(it) })

        val hdr = Codec.unpackHeader(pkt)!!
        assertEquals(type, hdr.type)
        assertEquals(seq, hdr.seq)
        assertEquals(ack, hdr.ack)
        assertEquals(length, hdr.length)
        assertEquals(reserved, hdr.reserved)
    }

    @Test
    fun syncReqRoundTrip() {
        val tv = loadVectors()
        @Suppress("UNCHECKED_CAST")
        val sample = (tv["sync_req"] as Map<String, Any?>)["sample"] as Map<String, Any?>
        val t1 = (sample["t1_ms"] as Number).toLong()
        val ver = (sample["proto_ver"] as Number).toInt()
        val payload = Codec.packSyncReq(t1, ver)
        assertEquals(9, payload.size)
        val (rt1, rver) = Codec.unpackSyncReq(payload)
        assertEquals(t1, rt1)
        assertEquals(ver, rver)
    }

    @Test
    fun syncAckRoundTrip() {
        val tv = loadVectors()
        @Suppress("UNCHECKED_CAST")
        val sample = (tv["sync_ack"] as Map<String, Any?>)["sample"] as Map<String, Any?>
        val t1 = (sample["t1"] as Number).toLong()
        val t2 = (sample["t2"] as Number).toLong()
        val t3 = (sample["t3"] as Number).toLong()
        val ver = (sample["proto_ver"] as Number).toInt()
        val payload = Codec.packSyncAck(t1, t2, t3, ver)
        assertEquals(25, payload.size)
        val arr = Codec.unpackSyncAck(payload)
        assertEquals(t1, arr[0]); assertEquals(t2, arr[1]); assertEquals(t3, arr[2])
        assertEquals(ver.toLong(), arr[3])
    }

    @Test
    fun syncCommitV2RoundTrip() {
        val tv = loadVectors()
        @Suppress("UNCHECKED_CAST")
        val sample = (tv["sync_commit_v2"] as Map<String, Any?>)["sample"] as Map<String, Any?>
        val tGoR = (sample["t_go_r"] as Number).toLong()
        val commitId = (sample["commit_id"] as Number).toLong()
        val payload = Codec.packSyncCommitV2(tGoR, commitId)
        assertEquals(16, payload.size)
        val (r1, r2) = Codec.unpackSyncCommitV2(payload)
        assertEquals(tGoR, r1); assertEquals(commitId, r2)
    }

    @Test
    fun punchRoundRoundTrip() {
        val tv = loadVectors()
        @Suppress("UNCHECKED_CAST")
        val sample = (tv["punch_round"] as Map<String, Any?>)["sample"] as Map<String, Any?>
        val roundNo = (sample["round_no"] as Number).toLong()
        val tGoR = (sample["t_go_r"] as Number).toLong()
        val payload = Codec.packPunchRound(roundNo, tGoR)
        assertEquals(16, payload.size)
        val (r1, r2) = Codec.unpackPunchRound(payload)
        assertEquals(roundNo, r1); assertEquals(tGoR, r2)
    }

    @Test
    fun portsAndConstantsMatch() {
        val tv = loadVectors()
        @Suppress("UNCHECKED_CAST")
        val ports = tv["ports"] as Map<String, Any?>
        assertEquals((ports["scan"] as Number).toInt(), C.SCAN_PORT)
        assertEquals((ports["lan_udp"] as Number).toInt(), C.LAN_UDP_PORT)
        assertEquals((ports["lan_tcp"] as Number).toInt(), C.LAN_TCP_PORT)
        assertEquals((ports["signal"] as Number).toInt(), C.DEFAULT_SERVER_PORT)
        assertEquals((ports["signal_tcp"] as Number).toInt(), C.DEFAULT_SERVER_TCP_PORT)
        assertEquals((ports["nat_probe"] as Number).toInt(), C.NAT_PROBE_PORT)
        assertEquals((ports["room_udp"] as Number).toInt(), C.ROOM_UDP_PORT)
        assertEquals((ports["room_tcp"] as Number).toInt(), C.ROOM_TCP_PORT)

        @Suppress("UNCHECKED_CAST")
        val types = tv["rtp_types"] as Map<String, Any?>
        assertEquals((types["DATA"] as Number).toInt(), C.RTP_DATA)
        assertEquals((types["ACK"] as Number).toInt(), C.RTP_ACK)
        assertEquals((types["FIN"] as Number).toInt(), C.RTP_FIN)
        assertEquals((types["KEEPALIVE"] as Number).toInt(), C.RTP_KEEPALIVE)
        assertEquals((types["SYNC_REQ"] as Number).toInt(), C.RTP_SYNC_REQ)
        assertEquals((types["SYNC_ACK"] as Number).toInt(), C.RTP_SYNC_ACK)
        assertEquals((types["SYNC_COMMIT"] as Number).toInt(), C.RTP_SYNC_COMMIT)
        assertEquals((types["PUNCH_ROUND"] as Number).toInt(), C.RTP_PUNCH_ROUND)
        assertEquals((types["TCP_READY"] as Number).toInt(), C.RTP_TCP_READY)
        assertEquals((types["PUNCH_FAIL"] as Number).toInt(), C.RTP_PUNCH_FAIL)
    }

    @Test
    fun jsonRoundTrip() {
        val obj = mapOf<String, Any?>(
            "type" to "join",
            "ver" to 1,
            "room" to "my_room",
            "lan" to listOf("192.168.1.2", "10.0.0.5"),
            "ok" to true,
            "empty" to null
        )
        val text = Json.encode(obj)
        val back = Json.decode(text)
        assertEquals("join", back["type"])
        assertEquals(1L, back["ver"])
        assertEquals("my_room", back["room"])
        @Suppress("UNCHECKED_CAST")
        val lan = back["lan"] as List<Any?>
        assertEquals(2, lan.size)
        assertEquals("192.168.1.2", lan[0])
        assertEquals(true, back["ok"])
        assertTrue(back.containsKey("empty"))
    }
}
