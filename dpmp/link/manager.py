# -*- coding: utf-8 -*-
"""连接管理：全连接 + 长连接 + 入站接管 + 保活。

把「双打洞协同」和「梯度冗余多路径」落到运行时：
    · 收 punch_go / TCP_READY / SYNC_COMMIT → 触发 TCP 打洞
    · UDP 打洞成功 → 建立 UDP-RTP 通道，并以它为控制面协调 TCP 打洞
    · 每条路径注册进 PeerPathScheduler，按角色分级调度与保活
    · 热备断裂 → 保守暖备上位 → 请求重建
"""

import socket
import threading
import time

from ..protocol import constants as C
from .puncher import HolePuncher
from .path import PeerPathScheduler
from .rtp import UdpReliableSocket


class Conn:
    """单条到某成员的长连接。io_lock 串行化收发。"""

    __slots__ = ("state", "sock", "addr", "member", "io_lock")

    def __init__(self, member):
        self.state = C.STATE_CONNECTING
        self.sock = None
        self.addr = None
        self.member = member
        self.io_lock = threading.Lock()


class LinkManager:
    """房间连接管理。

    参数：
      signaling：SignalingClient 实例（可空，局域网模式无信令）
      local_tcp_port：本机打洞/长连接 TCP 端口
      log：日志回调

    回调：
      on_socket_ready(peer_id, sock, member)   TCP 通道建立
      on_udp_ready(peer_id, rtp)               UDP-RTP 通道建立
      on_state_changed()                       成员状态变化（UI 刷新）
    """

    def __init__(self, signaling, local_tcp_port, log=None, config=None):
        if config is None:
            from ..config import DEFAULT_CONFIG
            config = DEFAULT_CONFIG
        self.cfg = config
        self.signaling = signaling
        self.local_tcp_port = local_tcp_port
        self.log = log or (lambda msg: None)
        self.lock = threading.Lock()
        self.members = {}
        self.connections = {}
        self.udp_conns = {}
        self._punch_sem = threading.Semaphore(config.punch_concurrency)
        self._puncher = HolePuncher(local_tcp_port, log=self.log, config=config)
        self._running = False
        self._keepalive_thread = None
        self.on_socket_ready = None
        self.on_udp_ready = None
        self.on_state_changed = None
        self._rebuild_cooldown = {}
        self._rebuild_cooldown_lock = threading.Lock()
        self._rebuild_cooldown_sec = config.rebuild_cooldown_sec
        self._punch_round_cond = threading.Condition(threading.Lock())
        self._punch_round_evt = {}
        self._mapping_ready = False
        self._path_schedulers = {}
        self._punch_active = {}
        self._peer_last_active = {}
        self._recv_epoch = {}
        # 网络切换重建冷却时间戳
        self._network_rebind_last = 0.0

    # ---------- 生命周期 ----------

    def start(self):
        self._running = True
        self._keepalive_thread = threading.Thread(target=self._keepalive_loop, daemon=True)
        self._keepalive_thread.start()

    def stop(self):
        self._running = False
        with self.lock:
            for conn in self.connections.values():
                self._close_sock(conn)
            self.connections.clear()
            for rtp in self.udp_conns.values():
                try:
                    rtp.close()
                except Exception:
                    pass
            self.udp_conns.clear()

    # ---------- 路径调度 ----------

    def _get_path_sched(self, peer_id):
        with self.lock:
            s = self._path_schedulers.get(peer_id)
            if s is None:
                s = PeerPathScheduler(peer_id, log=self.log, config=self.cfg)
                self._path_schedulers[peer_id] = s
            return s

    def mark_mapping_ready(self):
        """TCP 映射就绪 → 向所有已有 UDP 通道广播 TCP_READY。"""
        self._mapping_ready = True
        self.broadcast_tcp_ready()

    # ---------- 双打洞：UDP 通道建立 ----------

    def on_udp_hole_ready(self, peer_id, peer_addr):
        """UDP 打洞成功：建立可靠 UDP 通道（共享 socket 模式）。"""
        sig = self.signaling
        if sig is None:
            self.log("[UDP-RTP] signaling 为空，跳过")
            return
        with self.lock:
            if peer_id in self.udp_conns:
                return
        try:
            def _send(addr, data):
                sig.send_udp_to(addr, data)
            try:
                self._puncher.set_udp_hint(peer_id, peer_addr[0], peer_addr[1])
            except Exception:
                pass

            def _on_dead(addr):
                self._handle_udp_peer_dead(peer_id)

            def _on_sync(t_go):
                try:
                    rtt = rtp.get_sync_rtt()
                    if rtt:
                        self._puncher.set_adaptive_timeout(rtt)
                except Exception:
                    pass
                self._on_sync_ready(peer_id, t_go)

            def _on_round(round_no, t_go_r):
                self._on_punch_round(peer_id, round_no, t_go_r)

            def _on_tcp_ready():
                self._on_peer_tcp_ready(peer_id)

            def _on_punch_fail():
                self._on_peer_punch_fail(peer_id)

            rtp = UdpReliableSocket(_send, peer_addr, log=self.log,
                                    config=self.cfg,
                                    on_peer_dead=_on_dead,
                                    on_sync_ready=_on_sync,
                                    on_punch_round=_on_round,
                                    on_tcp_ready=_on_tcp_ready,
                                    on_punch_fail=_on_punch_fail)
        except Exception as e:
            self.log("[UDP-RTP] 创建失败: %s" % e)
            return
        try:
            peer_key = "%s:%d" % (peer_addr[0], peer_addr[1])
            sig.register_udp_rtp(peer_key, rtp.on_packet)
        except Exception as e:
            self.log("[UDP-RTP] 注册接收回调失败: %s" % e)
            return
        with self.lock:
            self.udp_conns[peer_id] = rtp
            member = dict(self.members.get(peer_id, {}))
        self.log("[UDP-RTP] 已为 %s 建立可靠通道 %s"
                 % (member.get("name", peer_id), peer_addr))
        try:
            self._get_path_sched(peer_id).register("udp:%s" % peer_id, "udp", 0)
        except Exception:
            pass
        cb = self.on_udp_ready
        if cb:
            try:
                cb(peer_id, rtp)
            except Exception as e:
                self.log("[UDP-RTP] 回调异常: %s" % e)
        if self._mapping_ready:
            try:
                rtp.send_tcp_ready()
                self.log("[打洞] 新通道通知 TCP_READY -> peer=%s" % peer_id)
            except Exception:
                pass
        try:
            my_id = getattr(sig, "my_id", None) or ""
            if my_id and my_id < peer_id:
                threading.Thread(target=self._maybe_start_sync,
                                 args=(peer_id, rtp), daemon=True).start()
        except Exception as e:
            self.log("[SYNC] 启动判断异常: %s" % e)

    def _maybe_start_sync(self, peer_id, rtp):
        try:
            time.sleep(0.3)
            with self.lock:
                conn = self.connections.get(peer_id)
                if conn and conn.state == C.STATE_CONNECTED:
                    return
            self.log("[SYNC] 作为 initiator 向 peer=%s 发起 SYNC" % peer_id)
            rtp.start_sync()
        except Exception as e:
            self.log("[SYNC] 异常: %s" % e)

    def _on_sync_ready(self, peer_id, t_go):
        with self.lock:
            conn = self.connections.get(peer_id)
            if conn and conn.state == C.STATE_CONNECTED:
                return
            peer = self.members.get(peer_id)
        if not peer:
            return
        threading.Thread(target=self._sync_punch_task,
                         args=(peer_id, peer, t_go), daemon=True).start()

    def _sync_punch_task(self, peer_id, peer, t_go):
        """单次精准 TCP 打洞（不重试，因为 SYNC 已对齐时刻）。"""
        try:
            now_ms = int(time.time() * 1000)
            wait_sec = (t_go - now_ms) / 1000.0
            if wait_sec > 0:
                time.sleep(min(wait_sec, 3.0))
            result = self._puncher.punch(peer, 0)
            if result:
                self._install_socket(peer_id, result)
                self.log("[SYNC] 精准 TCP 打洞成功 peer=%s" % peer_id)
            else:
                self.log("[SYNC] 精准 TCP 打洞失败 peer=%s，转入多轮重试" % peer_id)
                self._punch_task(peer, 0)
        except Exception as e:
            self.log("[SYNC] TCP 打洞异常: %s" % e)

    def get_udp_socket(self, peer_id):
        with self.lock:
            return self.udp_conns.get(peer_id)

    def broadcast_tcp_ready(self):
        with self.lock:
            items = list(self.udp_conns.items())
        for pid, rtp in items:
            try:
                rtp.send_tcp_ready()
                self.log("[打洞] 广播 TCP_READY -> peer=%s" % pid)
            except Exception:
                pass

    def _on_peer_tcp_ready(self, peer_id):
        with self.lock:
            peer = self.members.get(peer_id)
            conn = self.connections.get(peer_id)
            if conn and conn.state in (C.STATE_CONNECTING, C.STATE_CONNECTED):
                return
            if not peer:
                return
            self.connections[peer_id] = Conn(peer)
        self.log("[打洞] 对端 TCP 就绪，立即发起打洞 peer=%s" % peer_id)
        threading.Thread(target=self._punch_task, args=(peer, 0), daemon=True).start()

    def _on_peer_punch_fail(self, peer_id):
        """对端放弃 TCP → 关闭本端【半开连接】并回退 UDP。

        关键：对端放弃说明它那条方向没打通。本端即便 connect 成功，也可能
        是【半开连接】——本端 SYN 到了对端，但对端 SYN 没到本端，对端应用层
        没有对应 socket 接收。这种连接本端 send() 能进内核缓冲，对端却收不到，
        数据会超时/RST。
        因此必须【无论本端是否 CONNECTED 都关闭 TCP】，否则它会被
        get_send_channel 选为发送通道，把文件发进一条单向死连接。
        """
        self.log("[打洞] 对端放弃 TCP，peer=%s 关闭半开连接并回退 UDP" % peer_id)
        with self.lock:
            conn = self.connections.get(peer_id)
            if conn:
                self._close_sock(conn)          # 关闭底层 socket，避免半开被选用
                conn.state = C.STATE_FAILED
        # 路径调度：热备失效 → 暖备上位
        try:
            self._on_path_failure(peer_id, "tcp")
        except Exception:
            pass
        cb = self.on_state_changed
        if cb:
            try:
                cb()
            except Exception:
                pass

    def _on_punch_round(self, peer_id, round_no, t_go_r):
        with self._punch_round_cond:
            self._punch_round_evt[peer_id] = (round_no, t_go_r)
            self._punch_round_cond.notify_all()

    def _wait_punch_round(self, peer_id, round_no, timeout):
        deadline = time.time() + timeout
        with self._punch_round_cond:
            while True:
                v = self._punch_round_evt.get(peer_id)
                if v and v[0] >= round_no:
                    return v[1]
                remain = deadline - time.time()
                if remain <= 0:
                    return None
                self._punch_round_cond.wait(remain)

    def _sleep_until_local(self, t_local_ms):
        remain = (t_local_ms - int(time.time() * 1000)) / 1000.0
        if remain > 0:
            time.sleep(min(remain, 3.0))

    def _on_path_failure(self, peer_id, proto):
        sched = self._path_schedulers.get(peer_id)
        if not sched:
            return
        if proto == "tcp":
            new_hot = sched.promote_on_hot_failure()
            if new_hot:
                self.log("[路径] peer=%s 热备失效，%s 上位为热备"
                         % (peer_id, new_hot))
        else:
            sched.remove("udp:%s" % peer_id)

    def new_recv_epoch(self, peer_key):
        with self.lock:
            e = self._recv_epoch.get(peer_key, 0) + 1
            self._recv_epoch[peer_key] = e
            return e

    def is_current_epoch(self, peer_key, epoch):
        with self.lock:
            return self._recv_epoch.get(peer_key) == epoch

    def on_network_changed(self):
        """宿主应用在检测到网络切换时调用（接口反转）。

        DPMP 是平台无关的库，无法自己感知网络变化（Wi-Fi/蜂窝切换、IP 变化
        是平台相关事件）。因此由宿主应用负责【检测】，检测到后调用本方法
        触发【重建】，重建逻辑由 DPMP 统一实现。

        编排完整重建：
          1) 关闭所有现存连接（旧网络的 NAT 映射已全部失效）
          2) 让信令客户端重开 socket 并重新加入（复用旧 id，若服务器支持）
          3) 对所有成员重新发起打洞

        带 10 秒冷却，避免网络抖动导致频繁重建。
        """
        with self.lock:
            if not self._running:
                return
        now = time.time()
        with self._rebuild_cooldown_lock:
            if now - self._network_rebind_last < 10.0:
                return
            self._network_rebind_last = now
        self.log("[网络] 检测到网络切换，重建所有连接")
        # 1) 关闭所有旧连接
        with self.lock:
            for conn in self.connections.values():
                self._close_sock(conn)
            self.connections.clear()
            for rtp in self.udp_conns.values():
                try:
                    rtp.close()
                except Exception:
                    pass
            self.udp_conns.clear()
            self._path_schedulers.clear()
        # 2) 信令重绑（重开 socket + 重新加入，复用旧 id）
        if self.signaling is not None:
            try:
                self.signaling.rebind()
            except Exception as e:
                self.log("[网络] 信令重建失败: %s" % e)
        # 3) 对所有成员重新发起打洞（清冷却，避免被挡）
        with self.lock:
            pids = list(self.members.keys())
        with self._rebuild_cooldown_lock:
            self._rebuild_cooldown.clear()
        for pid in pids:
            try:
                self._request_rebuild(pid)
            except Exception:
                pass

    def _request_rebuild(self, peer_id):
        now = time.time()
        with self._rebuild_cooldown_lock:
            last = self._rebuild_cooldown.get(peer_id, 0)
            if now - last < self._rebuild_cooldown_sec:
                self.log("[房间] peer=%s 重建冷却中（%.0f 秒前）"
                         % (peer_id, now - last))
                return
            self._rebuild_cooldown[peer_id] = now
        try:
            if self.signaling:
                self.log("[房间] peer=%s 请求重新打洞" % peer_id)
                self.signaling.request_punch(peer_id)
        except Exception as e:
            self.log("[房间] peer=%s 重新打洞请求失败: %s" % (peer_id, e))

    def _handle_udp_peer_dead(self, peer_id):
        self.log("[UDP-RTP] peer=%s 通道失联，清理并触发重建" % peer_id)
        try:
            self._on_path_failure(peer_id, "udp")
        except Exception:
            pass
        with self.lock:
            rtp = self.udp_conns.pop(peer_id, None)
        if rtp:
            try:
                rtp.close()
            except Exception:
                pass
            try:
                addr = rtp.peer
                peer_key = "%s:%d" % (addr[0], addr[1])
                if self.signaling:
                    self.signaling.unregister_udp_rtp(peer_key)
            except Exception:
                pass
        self._request_rebuild(peer_id)

    # ---------- 成员事件 ----------

    def on_joined(self, members):
        for m in members:
            self._add_member(m)

    def on_member_join(self, member):
        self._add_member(member)

    def on_member_leave(self, peer_id):
        with self.lock:
            self.members.pop(peer_id, None)
            conn = self.connections.pop(peer_id, None)
            if conn:
                self._close_sock(conn)
            self._path_schedulers.pop(peer_id, None)
            self._peer_last_active.pop(peer_id, None)
            rtp = self.udp_conns.pop(peer_id, None)
        if rtp:
            try:
                rtp.close()
            except Exception:
                pass
        with self._rebuild_cooldown_lock:
            self._rebuild_cooldown.pop(peer_id, None)
        self.log("[房间] 成员离开 %s" % peer_id)

    def on_punch_go(self, peer, at_ms):
        peer_id = peer.get("id")
        if not peer_id:
            return
        with self.lock:
            self.members[peer_id] = peer
            conn = self.connections.get(peer_id)
            if conn and conn.state in (C.STATE_CONNECTING, C.STATE_CONNECTED):
                return
            self.connections[peer_id] = Conn(peer)
        threading.Thread(target=self._punch_task, args=(peer, at_ms), daemon=True).start()

    def on_inbound(self, sock, addr):
        """处理 listener accept 到的连接：若匹配成员 TCP 公网映射则接管。

        回调必须在【释放锁之后】执行（get_conn 需要同一把锁，锁内回调会死锁）。
        """
        ip, port = addr[0], addr[1]
        key = "%s:%d" % (ip, port)
        ready_pid = None
        ready_member = None
        with self.lock:
            for pid, m in self.members.items():
                pub_tcp = m.get("pub_tcp", "")
                if pub_tcp and pub_tcp == key:
                    conn = self.connections.get(pid)
                    if conn and conn.state == C.STATE_CONNECTED and conn.sock:
                        return True
                    if conn:
                        self._close_sock(conn)
                    new_conn = Conn(m)
                    new_conn.state = C.STATE_CONNECTED
                    new_conn.sock = sock
                    new_conn.addr = (ip, port)
                    self.connections[pid] = new_conn
                    ready_pid = pid
                    ready_member = dict(m)
                    break
        if ready_pid is None:
            return False
        self._apply_keepalive(sock)
        self.log("[房间] 入站连接 %s <- %s" % (ready_member.get("name", ready_pid), key))
        cb = self.on_socket_ready
        if cb:
            try:
                cb(ready_pid, sock, ready_member)
            except Exception as e:
                self.log("[房间] 入站回调异常: %s" % e)
        return True

    # ---------- 通道查询 ----------

    def get_socket(self, peer_id):
        with self.lock:
            conn = self.connections.get(peer_id)
            if conn and conn.state == C.STATE_CONNECTED and conn.sock:
                return conn.sock
        return None

    def _channel_usable(self, sock):
        if sock is None:
            return False
        if getattr(sock, "is_udp_rtp", False):
            try:
                return not sock._closed
            except Exception:
                return True
        try:
            return sock.fileno() != -1
        except Exception:
            return False

    def mark_active(self, peer_id):
        with self.lock:
            self._peer_last_active[peer_id] = time.time()

    def _idle_factor(self, peer_id, now):
        last = self._peer_last_active.get(peer_id, now)
        idle = now - last
        for threshold, factor in self.cfg.idle_factor_tiers:
            if idle < threshold:
                return factor
        return self.cfg.idle_factor_max

    def get_send_channel(self, peer_id, exclude_socks=None):
        """按路径角色选路发送：优先热备，其次保守暖备，最后宽松暖备。

        返回 (sock, io_lock, is_udp, role) 或 None。
        """
        exclude = exclude_socks or set()
        sched = self._path_schedulers.get(peer_id)
        if sched is not None:
            for role in (C.ROLE_HOT, C.ROLE_WARM_SAFE, C.ROLE_WARM_LOOSE):
                with sched.lock:
                    path = next((p for p in sched.paths.values()
                                 if p.role == role), None)
                if path is None:
                    continue
                if path.proto == "tcp":
                    with self.lock:
                        conn = self.connections.get(peer_id)
                        if (conn and conn.state == C.STATE_CONNECTED and conn.sock
                                and id(conn.sock) not in exclude
                                and self._channel_usable(conn.sock)):
                            return (conn.sock, conn.io_lock, False, role)
                else:
                    with self.lock:
                        rtp = self.udp_conns.get(peer_id)
                        if (rtp is not None and id(rtp) not in exclude
                                and self._channel_usable(rtp)):
                            return (rtp, rtp.io_lock, True, role)
        with self.lock:
            conn = self.connections.get(peer_id)
            if (conn and conn.state == C.STATE_CONNECTED and conn.sock
                    and id(conn.sock) not in exclude
                    and self._channel_usable(conn.sock)):
                return (conn.sock, conn.io_lock, False, "?")
            rtp = self.udp_conns.get(peer_id)
            if (rtp is not None and id(rtp) not in exclude
                    and self._channel_usable(rtp)):
                return (rtp, rtp.io_lock, True, "?")
        return None

    def get_path_roles(self, peer_id):
        sched = self._path_schedulers.get(peer_id)
        if not sched:
            return {}
        with sched.lock:
            return {pid: p.role for pid, p in sched.paths.items()}

    def has_peer(self, peer_id):
        with self.lock:
            return peer_id in self.connections

    def get_conn(self, peer_id):
        with self.lock:
            return self.connections.get(peer_id)

    def get_peer_did(self, peer_id):
        with self.lock:
            m = self.members.get(peer_id)
            if m and m.get("did"):
                return m["did"]
        return peer_id

    def get_members(self):
        with self.lock:
            out = []
            for pid, m in self.members.items():
                conn = self.connections.get(pid)
                state = conn.state if conn else "idle"
                addr = conn.addr if conn else None
                if state != C.STATE_CONNECTED and pid in self.udp_conns:
                    state = C.STATE_CONNECTED
                    rtp = self.udp_conns[pid]
                    addr = "udp://%s:%d" % rtp.peer
                roles = {}
                sched = self._path_schedulers.get(pid)
                if sched:
                    with sched.lock:
                        roles = {p.path_id: p.role for p in sched.paths.values()}
                out.append({
                    "id": pid,
                    "name": m.get("name", ""),
                    "state": state,
                    "addr": addr,
                    "path_roles": roles,
                })
            return out

    def _add_member(self, member):
        peer_id = member.get("id")
        if not peer_id:
            return
        with self.lock:
            self.members[peer_id] = member
        try:
            self.signaling.request_punch(peer_id)
        except Exception as e:
            self.log("[房间] 请求打洞失败: %s" % e)

    # ---------- 打洞任务 ----------

    def _punch_task(self, peer, at_ms):
        """多轮重试打洞。

        每轮开始前【重读】self.members 里的最新 peer——对端的 pub_tcp
        可能在上一次 punch_go 之后才登记。
        """
        peer_id = peer.get("id")
        with self.lock:
            cnt = self._punch_active.get(peer_id, 0)
            if cnt >= 2:
                return
            self._punch_active[peer_id] = cnt + 1
        self._punch_sem.acquire()
        try:
            for attempt in range(1, self.cfg.punch_retry + 1):
                if not self._running:
                    return
                with self.lock:
                    _c = self.connections.get(peer_id)
                    if _c and _c.state == C.STATE_CONNECTED:
                        self.log("[打洞] peer=%s 已由其他任务连接，本任务退出" % peer_id)
                        return
                with self.lock:
                    latest = self.members.get(peer_id) or peer
                if attempt == 1:
                    self.log("[打洞] 任务启动 peer=%s 本地端口=%d"
                             % (peer_id, self._puncher.local_tcp_port))
                else:
                    self.log("[打洞] 第 %d 轮重试 peer=%s（pub_tcp=%s）"
                             % (attempt, peer_id, latest.get("pub_tcp", "")))
                result = self._puncher.punch(latest, at_ms)
                if result:
                    self._install_socket(peer_id, result)
                    return
                if attempt < self.cfg.punch_retry:
                    next_round = attempt + 1
                    backoff = attempt * self.cfg.punch_retry_backoff
                    rtp = self.get_udp_socket(peer_id)
                    my_id = getattr(self.signaling, "my_id", None) or ""
                    if rtp is not None and my_id and my_id < peer_id:
                        t_go_next_i = int(time.time() * 1000) + backoff * 1000
                        off = rtp.get_sync_offset() or 0
                        rtp.send_punch_round(next_round, t_go_next_i + off)
                        self.log("[打洞] 主导轮次 %d，约定 T_go(本地)=%d"
                                 % (next_round, t_go_next_i))
                        self._sleep_until_local(t_go_next_i)
                        at_ms = 0
                    elif rtp is not None and my_id and my_id > peer_id:
                        t_go_r = self._wait_punch_round(peer_id, next_round, backoff + 2.0)
                        if t_go_r is not None:
                            self.log("[打洞] 跟随主导轮次 %d，T_go(本地)=%d"
                                     % (next_round, t_go_r))
                            self._sleep_until_local(t_go_r)
                        else:
                            time.sleep(backoff)
                        at_ms = 0
                    else:
                        time.sleep(backoff)
                        at_ms = 0
            mark_failed = False
            with self.lock:
                conn = self.connections.get(peer_id)
                if conn and conn.state != C.STATE_CONNECTED:
                    conn.state = C.STATE_FAILED
                    mark_failed = True
            if not mark_failed:
                self.log("[房间] 本任务失败，但 peer=%s 已由其他任务连接，忽略"
                         % peer_id)
                return
            self.log("[房间] 连接失败 %s" % peer.get("name", peer_id))
            try:
                rtp = self.get_udp_socket(peer_id)
                if rtp is not None:
                    rtp.send_punch_fail()
            except Exception:
                pass
        finally:
            with self.lock:
                c = self._punch_active.get(peer_id, 1) - 1
                if c <= 0:
                    self._punch_active.pop(peer_id, None)
                else:
                    self._punch_active[peer_id] = c
            self._punch_sem.release()

    def _install_socket(self, peer_id, result):
        with self.lock:
            old = self.connections.get(peer_id)
            if old and old.state == C.STATE_CONNECTED and old.sock:
                try:
                    result.sock.close()
                except Exception:
                    pass
                return
            if old:
                self._close_sock(old)
            conn = Conn(self.members.get(peer_id, {}))
            conn.state = C.STATE_CONNECTED
            conn.sock = result.sock
            conn.addr = (result.ip, result.port)
            self.connections[peer_id] = conn
            member = dict(self.members.get(peer_id, {}))
        self._apply_keepalive(result.sock)
        self.log("[房间] 已连接 %s -> %s:%d"
                 % (member.get("name", peer_id), result.ip, result.port))
        try:
            self._get_path_sched(peer_id).register("tcp:%s" % peer_id, "tcp", 0)
        except Exception:
            pass
        cb = self.on_socket_ready
        if cb:
            try:
                cb(peer_id, result.sock, member)
            except Exception as e:
                self.log("[房间] 接收回调异常: %s" % e)
        sc = self.on_state_changed
        if sc:
            try:
                sc()
            except Exception:
                pass

    def _apply_keepalive(self, sock):
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except Exception:
            pass

    def _close_sock(self, conn):
        if conn and conn.sock:
            try:
                conn.sock.close()
            except Exception:
                pass
            conn.sock = None

    # ---------- 保活循环 ----------

    def _keepalive_loop(self):
        _next_due = {}
        while self._running:
            time.sleep(self.cfg.keepalive_tick)
            if not self._running:
                break
            now = time.time()
            state_changed = False

            for pid in list(self._path_schedulers.keys()):
                sched = self._path_schedulers.get(pid)
                if not sched:
                    continue
                with sched.lock:
                    items = list(sched.paths.items())
                idle_factor = self._idle_factor(pid, now)
                live_ids = set()
                for path_id, p in items:
                    if p.role == C.ROLE_DEAD:
                        continue
                    live_ids.add(path_id)
                    if now < _next_due.get(path_id, 0):
                        continue
                    ok = self._probe_path(pid, path_id, p)
                    if ok:
                        p.scheduler.on_success()
                        p.last_seen = now
                    else:
                        p.scheduler.on_failure()
                        self.log("[保活] 路径 %s 失败（间隔=%ds）"
                                 % (path_id, p.scheduler.current_interval))
                    _next_due[path_id] = now + p.scheduler.current_interval * idle_factor
                for stale_id in [k for k in _next_due if k not in live_ids]:
                    _next_due.pop(stale_id, None)

            with self.lock:
                tcp_items = [(pid, conn) for pid, conn in self.connections.items()
                             if conn.state == C.STATE_CONNECTED]
            for pid, conn in tcp_items:
                if not self._alive(conn.sock):
                    self.log("[保活] %s TCP 通道失效，重新打洞" % pid)
                    dead = False
                    with self.lock:
                        cur = self.connections.get(pid)
                        if cur is conn:
                            self._close_sock(cur)
                            cur.state = C.STATE_FAILED
                            dead = True
                    if not dead:
                        continue
                    try:
                        self._on_path_failure(pid, "tcp")
                    except Exception:
                        pass
                    self._request_rebuild(pid)
                    state_changed = True

            with self.lock:
                udp_items = list(self.udp_conns.items())
            for pid, rtp in udp_items:
                try:
                    if rtp.is_dead():
                        self.log("[保活] %s UDP-RTP 通道失联" % pid)
                        self._handle_udp_peer_dead(pid)
                        state_changed = True
                except Exception:
                    pass

            if state_changed:
                cb = self.on_state_changed
                if cb:
                    try:
                        cb()
                    except Exception:
                        pass

    def _probe_path(self, peer_id, path_id, path):
        if path.proto == "tcp":
            with self.lock:
                conn = self.connections.get(peer_id)
            if conn and conn.state == C.STATE_CONNECTED and conn.sock:
                return self._alive(conn.sock)
            return False
        else:
            with self.lock:
                rtp = self.udp_conns.get(peer_id)
            if rtp is None:
                return False
            try:
                return not rtp.is_dead()
            except Exception:
                return False

    def _alive(self, sock):
        if not sock:
            return False
        try:
            sock.setblocking(False)
        except Exception:
            return False
        try:
            data = sock.recv(1, socket.MSG_PEEK)
            return data != b""
        except BlockingIOError:
            return True
        except Exception:
            return False
        finally:
            try:
                sock.setblocking(True)
            except Exception:
                pass
