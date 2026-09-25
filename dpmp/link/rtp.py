# -*- coding: utf-8 -*-
"""UDP-RTP：DPMP 的可靠 UDP 字节流通道（共享 socket 模式）。

在 DPMP 中的角色：
    · 双打洞（Dual-Punch）里，UDP 打洞成功后建立 UDP-RTP 通道；
    · 该通道既是数据通道，也是 TCP 打洞的【控制面】——
      承载 SYNC_REQ/ACK/COMMIT、PUNCH_ROUND、TCP_READY、PUNCH_FAIL。

架构：
    · 一个本地 UDP socket 被多个对端共享；
    · 全局接收循环（由上层 manager 维护）从 socket 收包，
      按源地址找到对应的 UdpReliableSocket，调用其 on_packet()；
    · 本类只负责：发送（通过 send_fn）、序号/ACK/重传、按序重组，
      以及 SYNC / 打洞协调消息的收发。

与旧实现（udp_reliable.py）的差异：
    · 常量统一从 dpmp.protocol.constants 取，不再本地硬编码；
    · 报文编解码统一走 dpmp.protocol.codec。
"""

import struct
import threading
import time

from ..protocol import constants as C
from ..protocol import codec as _codec

# 兼容旧引用名
MAX_PAYLOAD = C.RTP_MAX_PAYLOAD
WINDOW = C.RTP_WINDOW
RTO_MS = C.RTP_RTO_MS
RTX_INTERVAL = C.RTP_RTX_INTERVAL
KEEPALIVE_INTERVAL = C.RTP_KEEPALIVE_INTERVAL
PEER_DEAD_TIMEOUT = C.RTP_PEER_DEAD_TIMEOUT
SYNC_SAMPLE_COUNT = C.SYNC_SAMPLE_COUNT
SYNC_TIMEOUT = C.SYNC_TIMEOUT
SYNC_COMMIT_DELAY_MS = C.SYNC_COMMIT_DELAY_MS
SYNC_COMMIT_RESEND = C.SYNC_COMMIT_RESEND
SYNC_COMMIT_RESEND_INTERVAL = C.SYNC_COMMIT_RESEND_INTERVAL
PROTO_VER = C.RTP_PROTO_VER


class UdpReliableSocket:
    """可靠 UDP 字节流（共享 socket 模式）。

    参数：
      send_fn(peer_addr, data)：发送一个 UDP 包到 peer_addr
      peer_addr：对端的 (ip, port)

    类属性 is_udp_rtp=True：供上层识别传输类型（TCP vs UDP）。
    """

    is_udp_rtp = True

    def __init__(self, send_fn, peer_addr, log=None, on_peer_dead=None,
                 on_sync_ready=None, on_punch_round=None,
                 on_tcp_ready=None, on_punch_fail=None, config=None):
        if config is None:
            from ..config import DEFAULT_CONFIG
            config = DEFAULT_CONFIG
        self.cfg = config
        self.max_payload = config.rtp_max_payload
        self.window = config.rtp_window_init   # 自适应发送窗口（受 _send_lock 保护）
        self._window_min = config.rtp_window_min
        self._window_max = config.rtp_window_max
        self.rto_ms = config.rtp_rto_ms
        self.rtx_interval = config.rtp_rtx_interval
        self.keepalive_interval = config.rtp_keepalive_interval
        self.peer_dead_timeout = config.rtp_peer_dead_timeout
        self.io_lock = threading.Lock()
        self._send_fn = send_fn
        self.peer = peer_addr
        self.log = log or (lambda m: None)
        self._on_peer_dead = on_peer_dead
        self._on_sync_ready = on_sync_ready
        self._on_punch_round = on_punch_round
        self._on_tcp_ready = on_tcp_ready
        self._on_punch_fail = on_punch_fail
        self._sync_offset = None
        self._sync_rtt = None
        self._peer_ver = 1
        self._commit_seq = 0
        self._last_commit_id = -1
        self._commit_lock = threading.Lock()

        self._send_seq = 0
        self._send_queue = {}
        self._send_lock = threading.Lock()

        self._recv_next = 0
        self._recv_buf = bytearray()
        self._out_of_order = {}
        self._recv_cond = threading.Condition()

        self._closed = False
        self._peer_closed = False
        self._timeout = None

        self._last_send_time = time.time()
        self._last_recv_time = time.time()
        self._peer_dead_fired = False

        self._sync_lock = threading.Lock()
        self._sync_in_progress = False
        self._sync_samples = []
        self._sync_pending = {}
        self._sync_replies = {}

        self._rtx_thread = threading.Thread(target=self._rtx_loop, daemon=True)
        self._rtx_thread.start()
        self._ka_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
        self._ka_thread.start()

    # ---------- 对外接口 ----------

    def sendall(self, data):
        if not data or self._closed:
            return
        offset = 0
        while offset < len(data):
            chunk = data[offset:offset + self.max_payload]
            offset += len(chunk)
            with self._send_lock:
                seq = self._send_seq
                self._send_seq += 1
                hdr = _codec.pack_header(C.RTP_DATA, seq, self._recv_next, len(chunk), 0)
                pkt = hdr + chunk
                self._send_queue[seq] = [pkt, time.time()]
                while len(self._send_queue) >= self.window and not self._closed:
                    self._send_lock.release()
                    time.sleep(0.005)
                    self._send_lock.acquire()
            try:
                self._send_fn(self.peer, pkt)
                self._last_send_time = time.time()
            except Exception as e:
                self.log("[UDP-RTP] send 失败: %s" % e)
                return

    def recv(self, n):
        with self._recv_cond:
            deadline = time.time() + self._timeout if self._timeout else None
            while len(self._recv_buf) == 0 and not self._peer_closed:
                if deadline is None:
                    self._recv_cond.wait(0.5)
                else:
                    remain = deadline - time.time()
                    if remain <= 0:
                        raise TimeoutError("recv timeout")
                    self._recv_cond.wait(remain)
            if not self._recv_buf:
                return b""
            take = min(n, len(self._recv_buf))
            data = bytes(self._recv_buf[:take])
            del self._recv_buf[:take]
            return data

    def recv_into(self, buffer, nbytes=0):
        if nbytes <= 0:
            nbytes = len(buffer)
        data = self.recv(nbytes)
        if not data:
            return 0
        buffer[:len(data)] = data
        return len(data)

    def recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("UDP-RTP 对端关闭")
            buf.extend(chunk)
        return bytes(buf)

    def settimeout(self, t):
        self._timeout = t

    def send_punch_round(self, round_no, t_go_r):
        """主导方向对端下发下一轮打洞时刻（responder 钟绝对毫秒）。"""
        try:
            payload = _codec.pack_punch_round(round_no, t_go_r)
            self._send_fn(self.peer,
                          _codec.pack_packet(C.RTP_PUNCH_ROUND, 0, self._recv_next, payload))
            self._last_send_time = time.time()
            return True
        except Exception as e:
            self.log("[打洞] 发送 PUNCH_ROUND 失败: %s" % e)
            return False

    def send_tcp_ready(self):
        """告知对端本机 TCP 映射已就绪，可开始打洞。"""
        try:
            self._send_fn(self.peer,
                          _codec.pack_packet(C.RTP_TCP_READY, 0, self._recv_next, b""))
            self._last_send_time = time.time()
            return True
        except Exception as e:
            self.log("[打洞] 发送 TCP_READY 失败: %s" % e)
            return False

    def send_punch_fail(self):
        """告知对端本机 TCP 打洞彻底失败，避免对端空等。"""
        try:
            self._send_fn(self.peer,
                          _codec.pack_packet(C.RTP_PUNCH_FAIL, 0, self._recv_next, b""))
            self._last_send_time = time.time()
            return True
        except Exception as e:
            self.log("[打洞] 发送 PUNCH_FAIL 失败: %s" % e)
            return False

    def get_sync_offset(self):
        return self._sync_offset

    def get_sync_rtt(self):
        return self._sync_rtt

    def get_peer_ver(self):
        return self._peer_ver

    def start_sync(self):
        """发起 SYNC 采样，完成后发送 SYNC_COMMIT 约定 TCP 打洞时刻。

        阻塞在调用线程，直至采样完成（约 1-4 秒）。
        """
        with self._sync_lock:
            if self._sync_in_progress:
                self.log("[SYNC] 已有采样在进行，跳过")
                return
            self._sync_in_progress = True
            self._sync_samples = []
            self._sync_pending = {}
            self._sync_replies = {}
        try:
            for i in range(self.cfg.sync_sample_count):
                if self._closed:
                    return
                t1 = int(time.time() * 1000)
                evt = threading.Event()
                with self._sync_lock:
                    self._sync_pending[t1] = evt
                try:
                    payload = _codec.pack_sync_req(t1, PROTO_VER)
                    self._send_fn(self.peer,
                                  _codec.pack_packet(C.RTP_SYNC_REQ, 0, self._recv_next, payload))
                    self._last_send_time = time.time()
                except Exception as e:
                    self.log("[SYNC] 发送 REQ 失败: %s" % e)
                    break
                ok = evt.wait(self.cfg.sync_timeout)
                with self._sync_lock:
                    reply = self._sync_replies.pop(t1, None)
                    self._sync_pending.pop(t1, None)
                if ok and reply:
                    rtt, offset, _t4 = reply
                    self._sync_samples.append((rtt, offset))
                    self.log("[SYNC] 采样 %d/%d: RTT=%dms offset=%dms"
                             % (i + 1, self.cfg.sync_sample_count, rtt, offset))
                else:
                    self.log("[SYNC] 采样 %d 超时" % (i + 1))
                time.sleep(0.05)

            if not self._sync_samples:
                self.log("[SYNC] 无有效采样，放弃 SYNC")
                return
            best_rtt, best_offset = min(self._sync_samples, key=lambda s: s[0])
            self._sync_offset = best_offset
            self._sync_rtt = best_rtt
            self.log("[SYNC] 最佳：RTT=%dms offset=%dms（共 %d 次采样，peer_ver=%d）"
                     % (best_rtt, best_offset, len(self._sync_samples), self._peer_ver))

            delta_ms = self.cfg.sync_commit_delay_ms
            t_send = int(time.time() * 1000)
            t_go = t_send + delta_ms
            best_offset = self._sync_offset or 0
            self._commit_seq += 1
            commit_id = self._commit_seq
            try:
                if self._peer_ver >= PROTO_VER:
                    t_go_r = t_go + best_offset
                    payload = _codec.pack_sync_commit_v2(t_go_r, commit_id)
                    for _ in range(self.cfg.sync_commit_resend):
                        self._send_fn(self.peer,
                                      _codec.pack_packet(C.RTP_SYNC_COMMIT, 0,
                                                         self._recv_next, payload))
                        self._last_send_time = time.time()
                        time.sleep(self.cfg.sync_commit_resend_interval)
                    self.log("[SYNC] 已发 COMMIT(v2) T_go_I=%d T_go_R=%d id=%d x%d"
                             % (t_go, t_go_r, commit_id, self.cfg.sync_commit_resend))
                else:
                    payload = _codec.pack_sync_commit_v1(delta_ms)
                    self._send_fn(self.peer,
                                  _codec.pack_packet(C.RTP_SYNC_COMMIT, 0,
                                                     self._recv_next, payload))
                    self._last_send_time = time.time()
                    self.log("[SYNC] 已发 COMMIT(v1) T_go=%d（%dms 后）" % (t_go, delta_ms))
            except Exception as e:
                self.log("[SYNC] 发送 COMMIT 失败: %s" % e)
                return

            cb = self._on_sync_ready
            if cb:
                try:
                    cb(t_go)
                except Exception as e:
                    self.log("[SYNC] on_sync_ready 回调异常: %s" % e)
        finally:
            with self._sync_lock:
                self._sync_in_progress = False

    def is_dead(self):
        if self._closed:
            return True
        if self._peer_dead_fired:
            return True
        return (time.time() - self._last_recv_time) > self.peer_dead_timeout

    def gettimeout(self):
        return self._timeout

    def getpeername(self):
        return self.peer

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._send_fn(self.peer,
                          _codec.pack_packet(C.RTP_FIN, 0, self._recv_next, b""))
        except Exception:
            pass
        with self._recv_cond:
            self._recv_cond.notify_all()

    # ---------- 接收（由全局接收循环调用） ----------

    def on_packet(self, data):
        """处理一个来自本对端的 UDP 包（已由上层按源地址过滤）。"""
        if len(data) < C.RTP_HEADER_SIZE or self._closed:
            return
        hdr = _codec.unpack_header(data)
        if hdr is None:
            return
        ptype, seq, ack, length, _ = hdr
        payload = data[C.RTP_HEADER_SIZE:C.RTP_HEADER_SIZE + length]
        self._last_recv_time = time.time()

        if ptype == C.RTP_DATA:
            self._on_data(seq, ack, payload)
        elif ptype == C.RTP_ACK:
            self._on_ack(ack)
        elif ptype == C.RTP_FIN:
            self._peer_closed = True
            with self._recv_cond:
                self._recv_cond.notify_all()
        elif ptype == C.RTP_KEEPALIVE:
            pass
        elif ptype == C.RTP_SYNC_REQ:
            self._on_sync_req(payload)
        elif ptype == C.RTP_SYNC_ACK:
            self._on_sync_ack(payload)
        elif ptype == C.RTP_SYNC_COMMIT:
            self._on_sync_commit(payload)
        elif ptype == C.RTP_PUNCH_ROUND:
            self._on_punch_round_pkt(payload)
        elif ptype == C.RTP_TCP_READY:
            cb = self._on_tcp_ready
            if cb:
                try:
                    cb()
                except Exception as e:
                    self.log("[打洞] on_tcp_ready 异常: %s" % e)
        elif ptype == C.RTP_PUNCH_FAIL:
            cb = self._on_punch_fail
            if cb:
                try:
                    cb()
                except Exception as e:
                    self.log("[打洞] on_punch_fail 异常: %s" % e)

    def _on_data(self, seq, ack, payload):
        self._on_ack(ack)
        try:
            self._send_fn(self.peer,
                          _codec.pack_header(C.RTP_ACK, 0, seq, 0, 0))
            self._last_send_time = time.time()
        except Exception:
            pass
        if seq == self._recv_next:
            self._recv_buf.extend(payload)
            self._recv_next += 1
            while self._recv_next in self._out_of_order:
                self._recv_buf.extend(self._out_of_order.pop(self._recv_next))
                self._recv_next += 1
            with self._recv_cond:
                self._recv_cond.notify_all()
        elif seq > self._recv_next:
            self._out_of_order[seq] = payload

    def _on_ack(self, ack):
        with self._send_lock:
            done = [s for s in self._send_queue if s < ack]
            advanced = len(done)
            for s in done:
                self._send_queue.pop(s, None)
            # AIMD 加性增：有确认推进 → 窗口 +advanced，封顶上限
            if advanced > 0:
                self.window = min(self._window_max, self.window + advanced)

    def _on_sync_req(self, payload):
        try:
            t1, peer_ver = _codec.unpack_sync_req(payload)
        except struct.error:
            return
        if peer_ver > self._peer_ver:
            self._peer_ver = peer_ver
        t2 = int(time.time() * 1000)
        t3 = t2
        try:
            ack_payload = _codec.pack_sync_ack(t1, t2, t3, PROTO_VER)
            self._send_fn(self.peer,
                          _codec.pack_packet(C.RTP_SYNC_ACK, 0, self._recv_next, ack_payload))
            self._last_send_time = time.time()
        except Exception as e:
            self.log("[SYNC] 发送 ACK 失败: %s" % e)

    def _on_sync_ack(self, payload):
        try:
            t1, t2, t3, peer_ver = _codec.unpack_sync_ack(payload)
        except struct.error:
            return
        if peer_ver > self._peer_ver:
            self._peer_ver = peer_ver
        t4 = int(time.time() * 1000)
        rtt = (t4 - t1) - (t3 - t2)
        offset = ((t2 - t1) + (t3 - t4)) // 2
        with self._sync_lock:
            if t1 in self._sync_pending:
                self._sync_replies[t1] = (rtt, offset, t4)
                self._sync_pending[t1].set()

    def _on_sync_commit(self, payload):
        """响应方收到 COMMIT：解析 T_go（本机钟）并回调上层。"""
        t_go_local = None
        try:
            if len(payload) == _codec._PAYLOAD_SYNC_COMMIT_V2.size:
                t_go_r, commit_id = _codec.unpack_sync_commit_v2(payload)
                with self._commit_lock:
                    if commit_id <= self._last_commit_id:
                        return  # 重复包（重传），幂等去重
                    self._last_commit_id = commit_id
                offset = self._sync_offset or 0
                t_go_local = t_go_r - offset
            else:
                (delta_ms,) = _codec.unpack_sync_commit_v1(payload)
                t_go_local = int(time.time() * 1000) + delta_ms
        except struct.error:
            return
        cb = self._on_sync_ready
        if cb and t_go_local is not None:
            try:
                cb(t_go_local)
            except Exception as e:
                self.log("[SYNC] on_sync_ready(响应方) 异常: %s" % e)

    def _on_punch_round_pkt(self, payload):
        try:
            round_no, t_go_r = _codec.unpack_punch_round(payload)
        except struct.error:
            return
        cb = self._on_punch_round
        if cb:
            try:
                cb(round_no, t_go_r)
            except Exception as e:
                self.log("[打洞] on_punch_round 异常: %s" % e)

    # ---------- 后台线程 ----------

    def _rtx_loop(self):
        while not self._closed:
            time.sleep(self.rtx_interval)
            now = time.time()
            resend = []
            with self._send_lock:
                for seq, (pkt, ts) in list(self._send_queue.items()):
                    if now - ts > self.rto_ms / 1000.0:
                        self._send_queue[seq][1] = now
                        resend.append(pkt)
                # AIMD 乘性减：出现超时重传（疑似丢包/拥塞）→ 窗口减半
                if resend:
                    self.window = max(self._window_min, self.window // 2)
            for pkt in resend:
                try:
                    self._send_fn(self.peer, pkt)
                    self._last_send_time = time.time()
                except Exception:
                    pass

    def _keepalive_loop(self):
        while not self._closed:
            time.sleep(1.0)
            now = time.time()
            if now - self._last_send_time >= self.keepalive_interval:
                try:
                    self._send_fn(self.peer,
                                  _codec.pack_header(C.RTP_KEEPALIVE, 0, self._recv_next, 0, 0))
                    self._last_send_time = now
                except Exception:
                    pass
            if (not self._peer_dead_fired
                    and now - self._last_recv_time > self.peer_dead_timeout):
                self._peer_dead_fired = True
                cb = self._on_peer_dead
                if cb:
                    try:
                        cb(self.peer)
                    except Exception:
                        pass
