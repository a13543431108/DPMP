# -*- coding: utf-8 -*-
"""双打洞核心（Dual-Punch）。

DPMP 的独创点之一：**双打洞协同**。
    · 第一层：UDP 打洞成功 → 建立 UDP-RTP 可靠通道；
    · 第二层：以上述 UDP 通道为控制面，协调 TCP 打洞
      （SYNC 轮次对齐 → 到点并行 connect 所有候选）。

本模块负责 TCP 打洞（TCP 同时打开）：
    · 候选地址构造（lan / pub_tcp / UDP 端口预测）
    · 到点并行 connect（Barrier 对齐起跑，谁先成功用谁）
    · 自适应 connect 超时（按 SYNC RTT 调整）
"""

import select
import socket
import threading
import time
from typing import Any, Callable, List, Optional, Tuple

from ..protocol import constants as C
from ..protocol.addr import parse_host_port


class PunchResult:
    """打洞成功的结果：一条保持打开的已连接 socket。"""

    def __init__(self, ip: str, port: int, sock: socket.socket) -> None:
        self.ip = ip
        self.port = port
        self.sock = sock


class HolePuncher:
    """对单个对端执行 TCP 打洞（TCP 同时打开）。

    参数：
      local_tcp_port：本机用于打洞的本地端口（双打洞需两端绑同一端口）
      log：日志回调
    """

    def __init__(self, local_tcp_port: int,
                 log: Optional[Callable[[str], None]] = None,
                 clock_offset_ms: int = 0, config=None) -> None:
        if config is None:
            from ..config import DEFAULT_CONFIG
            config = DEFAULT_CONFIG
        self.cfg = config
        self.local_tcp_port = local_tcp_port
        self.log = log or (lambda msg: None)
        # 双打洞：UDP 打洞成功后记录的对方 UDP 公网端口，用于预测 TCP 端口
        self.udp_hint = {}     # {peer_id: (ip, port)}
        self._hint_lock = threading.Lock()
        # 自适应 connect 超时（由 SYNC RTT 调整）
        self.connect_timeout = config.punch_connect_timeout
        # 本地时钟 与 参考时钟 的偏移（毫秒）：local = ref - offset
        self._clock_offset_ms = clock_offset_ms
        # 诊断开关：逐候选的失败/超时日志默认不输出
        self.verbose = False

    def set_clock_offset(self, offset_ms: int) -> None:
        """更新时钟偏移（由信令时间同步写入）。"""
        self._clock_offset_ms = offset_ms or 0

    def set_adaptive_timeout(self, rtt_ms: int) -> None:
        """按 SYNC RTT 调整 connect 超时（3~8 秒）。"""
        if not rtt_ms or rtt_ms <= 0:
            return
        t = max(3.0, min(8.0, rtt_ms / 1000.0 * 4.0))
        self.connect_timeout = t

    def set_udp_hint(self, peer_id: str, ip: str, port: int) -> None:
        """记录 UDP 打洞得到的对端公网地址，供预测 TCP 端口。"""
        with self._hint_lock:
            self.udp_hint[peer_id] = (ip, port)

    def punch(self, peer: dict, at_ms: int) -> Optional[PunchResult]:
        """执行打洞。peer 为 dict：{id, tcp, lan:[...], pub_tcp:"ip:port"}。

        返回 PunchResult 或 None。
        """
        tcp_port = peer.get("tcp") or 0
        if not tcp_port:
            self.log("[打洞] 放弃：对方 tcp 端口为 0 (peer=%s)" % peer.get("id"))
            return None
        candidates = self._build_candidates(peer)
        if not candidates:
            self.log("[打洞] 放弃：候选地址为空 (peer=%s)" % peer.get("id"))
            return None
        if self.verbose:
            self.log("[打洞] peer=%s 候选=%s"
                     % (peer.get("id"),
                        ", ".join("%s:%d" % (ip, p) for ip, p in candidates)))
        self._wait_until(at_ms)
        result = self._connect_candidates_parallel(candidates, peer.get("id"))
        if result:
            self.log("[打洞] 成功 -> %s:%d (peer=%s)"
                     % (result.ip, result.port, peer.get("id")))
            return result
        if self.verbose:
            self.log("[打洞] 失败 peer=%s（所有候选均不可达）" % peer.get("id"))
        return None

    def _connect_candidates_parallel(
            self, candidates: List[Tuple[str, int]],
            peer_id: str) -> Optional[PunchResult]:
        """并行尝试所有候选地址，返回首个成功的 PunchResult。"""
        n = len(candidates)
        if n == 1:
            return self._try_connect(candidates[0][0], candidates[0][1])
        barrier = threading.Barrier(n)
        results = {}
        done = threading.Event()
        win_lock = threading.Lock()
        # 限制同时 connect 的候选数（每个候选都绑同一本地端口），
        # 降低 SO_REUSEPORT 入站匹配冲突。
        conn_sem = threading.Semaphore(self.cfg.punch_candidate_concurrency)

        def worker(idx, ip, port):
            try:
                barrier.wait(timeout=3.0)
            except Exception:
                pass
            if done.is_set():
                return
            conn_sem.acquire()
            try:
                if done.is_set():
                    return
                r = self._try_connect(ip, port)
            finally:
                conn_sem.release()
            if r is None:
                return
            with win_lock:
                if done.is_set():
                    try:
                        r.sock.close()
                    except Exception:
                        pass
                    return
                results["win"] = r
                done.set()

        threads = []
        for i, (ip, port) in enumerate(candidates):
            t = threading.Thread(target=worker, args=(i, ip, port), daemon=True)
            t.start()
            threads.append(t)
        done.wait(self.connect_timeout + 2.0)
        for t in threads:
            t.join(timeout=0.2)
        return results.get("win")

    def _build_candidates(self, peer: dict) -> List[Tuple[str, int]]:
        tcp_port = peer.get("tcp") or 0
        result = []
        seen = set()
        for ip in peer.get("lan", []):
            if ip and (ip, tcp_port) not in seen:
                seen.add((ip, tcp_port))
                result.append((ip, tcp_port))
        pub_tcp = peer.get("pub_tcp", "")
        parsed = parse_host_port(pub_tcp)
        if parsed:
            pip, pport = parsed
            key = (pip, pport)
            if key not in seen:
                seen.add(key)
                result.append(key)
        # 双打洞端口预测：pub_tcp 为空时，用 UDP 打洞的公网端口 ±2 预测 TCP 候选
        if not parsed:
            with self._hint_lock:
                hint = self.udp_hint.get(peer.get("id"))
            if hint and ":" not in hint[0]:   # 仅 IPv4
                hip, hport = hint
                for dp in self.cfg.punch_port_predict_range:
                    pp = hport + dp
                    if pp <= 0 or pp > 65535:
                        continue
                    key = (hip, pp)
                    if key not in seen:
                        seen.add(key)
                        result.append(key)
                if self.verbose:
                    self.log("[打洞] 追加 UDP 预测候选 %s:%d±2 (peer=%s)"
                             % (hip, hport, peer.get("id")))
        return result

    def _wait_until(self, at_ms: int) -> None:
        """等到参考时钟 at_ms 时刻（换算成本地时刻）。"""
        if not at_ms:
            return
        local_target = (at_ms - self._clock_offset_ms) / 1000.0
        remain = local_target - time.time()
        if remain > 0:
            time.sleep(min(remain, 2.0))

    def _try_connect(self, ip: str, port: int) -> Optional[PunchResult]:
        """非阻塞 connect + select 等待 —— TCP 同时打开的正确实现。

        不能用 sock.settimeout()+connect_ex()：Windows 上 settimeout 会让
        socket 进入非阻塞模式，connect_ex 立即返回 WSAEWOULDBLOCK(10035)，
        不代表连接失败。必须用 select 等待可写后读 SO_ERROR。
        """
        family = socket.AF_INET6 if ":" in ip else socket.AF_INET
        sock = None
        bind_ok = False
        try:
            sock = socket.socket(family, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                if family == socket.AF_INET6:
                    sock.bind(("::", self.local_tcp_port))
                else:
                    sock.bind(("", self.local_tcp_port))
                bind_ok = True
            except Exception as be:
                self.log("[打洞] bind %d 失败: %s（继续，走内核随机端口）"
                         % (self.local_tcp_port, be))
            sock.setblocking(False)
            t0 = time.time()
            rc = sock.connect_ex((ip, port))
            if rc == 0:
                sock.setblocking(True)
                sock.settimeout(None)
                self.log("[打洞] connect 立即成功 %s:%d（bind=%s）" % (ip, port, bind_ok))
                return PunchResult(ip, port, sock)
            try:
                _, wlist, xlist = select.select([], [sock], [sock], self.connect_timeout)
            except Exception as se:
                self.log("[打洞] select 异常 %s:%d -> %s" % (ip, port, se))
                wlist, xlist = [], []
            dt_ms = int((time.time() - t0) * 1000)
            if not wlist and not xlist:
                if self.verbose:
                    self.log("[打洞] connect 超时 %s:%d（%dms, bind=%s, rc=%d）"
                             % (ip, port, dt_ms, bind_ok, rc))
            else:
                err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if err == 0:
                    sock.setblocking(True)
                    sock.settimeout(None)
                    self.log("[打洞] connect 成功 %s:%d（%dms, bind=%s）"
                             % (ip, port, dt_ms, bind_ok))
                    return PunchResult(ip, port, sock)
                if self.verbose:
                    self.log("[打洞] connect 失败 %s:%d errno=%d（%dms, bind=%s）"
                             % (ip, port, err, dt_ms, bind_ok))
        except Exception as e:
            if self.verbose:
                self.log("[打洞] connect 异常 %s:%d -> %s" % (ip, port, e))
        if sock:
            try:
                sock.close()
            except Exception:
                pass
        return None
