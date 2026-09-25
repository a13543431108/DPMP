# -*- coding: utf-8 -*-
"""DPMP 二进制编解码。

UDP-RTP 包头（大端）：
    [1B type][4B seq][4B ack][2B length][1B reserved] = 12 字节
"""

import struct
from . import constants as C

# UDP-RTP 12 字节包头：!B I I H B
RTP_HEADER = struct.Struct("!BIIHB")

# 各控制报文的 payload 结构
_PAYLOAD_SYNC_REQ = struct.Struct("!QB")      # t1(ms), proto_ver
_PAYLOAD_SYNC_ACK = struct.Struct("!QQQB")    # t1, t2, t3, proto_ver
_PAYLOAD_SYNC_COMMIT_V2 = struct.Struct("!QQ")  # t_go_r, commit_id
_PAYLOAD_SYNC_COMMIT_V1 = struct.Struct("!Q")   # delta_ms
_PAYLOAD_PUNCH_ROUND = struct.Struct("!QQ")     # round_no, t_go_r


def pack_header(ptype, seq=0, ack=0, length=0, reserved=0):
    return RTP_HEADER.pack(ptype, seq, ack, length, reserved)


def unpack_header(data):
    """解析 12 字节包头，返回 (type, seq, ack, length, reserved)。

    数据不足 12 字节时返回 None。
    """
    if len(data) < C.RTP_HEADER_SIZE:
        return None
    try:
        return RTP_HEADER.unpack(data[:C.RTP_HEADER_SIZE])
    except struct.error:
        return None


def pack_packet(ptype, seq, ack, payload=b""):
    """打包一个完整的 UDP-RTP 报文（头 + payload）。"""
    return pack_header(ptype, seq, ack, len(payload)) + payload


# ---------- SYNC_REQ ----------

def pack_sync_req(t1_ms, proto_ver=C.RTP_PROTO_VER):
    return _PAYLOAD_SYNC_REQ.pack(t1_ms, proto_ver)


def unpack_sync_req(payload):
    return _PAYLOAD_SYNC_REQ.unpack(payload)


# ---------- SYNC_ACK ----------

def pack_sync_ack(t1, t2, t3, proto_ver=C.RTP_PROTO_VER):
    return _PAYLOAD_SYNC_ACK.pack(t1, t2, t3, proto_ver)


def unpack_sync_ack(payload):
    return _PAYLOAD_SYNC_ACK.unpack(payload)


# ---------- SYNC_COMMIT ----------

def pack_sync_commit_v2(t_go_r, commit_id):
    return _PAYLOAD_SYNC_COMMIT_V2.pack(t_go_r, commit_id)


def unpack_sync_commit_v2(payload):
    return _PAYLOAD_SYNC_COMMIT_V2.unpack(payload)


def pack_sync_commit_v1(delta_ms):
    return _PAYLOAD_SYNC_COMMIT_V1.pack(delta_ms)


def unpack_sync_commit_v1(payload):
    return _PAYLOAD_SYNC_COMMIT_V1.unpack(payload)


# ---------- PUNCH_ROUND ----------

def pack_punch_round(round_no, t_go_r):
    return _PAYLOAD_PUNCH_ROUND.pack(round_no, t_go_r)


def unpack_punch_round(payload):
    return _PAYLOAD_PUNCH_ROUND.unpack(payload)
