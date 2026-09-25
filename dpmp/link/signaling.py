# -*- coding: utf-8 -*-
"""UDP 信令客户端：加入房间、心跳、接收成员/打洞事件。

公网房间模式的核心。服务器只做「牵线」——交换地址、协调打洞时刻，
不承载任何数据流量。

职责：
    · 加入房间（join）与心跳（hb）
    · NTP 式时间同步（与服务器对齐时钟，供打洞时刻使用）
    · TCP 映射观测（_open_mapping，暴露本机 TCP 公网映射）
    · UDP 打洞 socket 维护 + 全局接收循环（UDP-RTP 分发的入口）
    · 接收成员上下线、punch_go 事件并回调上层
"""

import json
import socket
import struct
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ..protocol import constants as C
from ..protocol.addr import parse_host_port
from . import natprobe


def _parse_server_spec(spec: Any, default_tcp: int,
                       default_nat: int) -> Optional[Tuple[str, int, int, int]]:
    """把一个服务器描述规范化成 (ip, port, tcp_port, nat_port)。

    支持写法：
      "ip:port" / "[ipv6]:port"
      (ip, port)
      (ip, port, tcp_port, nat_probe_port)
      {"ip":.., "port":.., "tcp_port":.., "nat_probe_port":..}
    无法解析返回 None。
    """
    ip = port = None
    tcp = default_tcp
    nat = default_nat
    if isinstance(spec, str):
        parsed = parse_host_port(spec)
        if not parsed:
            return None
        ip, port = parsed
    elif isinstance(spec, (tuple, list)):
        if len(spec) >= 2:
            ip, port = spec[0], int(spec[1])
        if len(spec) >= 3 and spec[2]:
            tcp = int(spec[2])
        if len(spec) >= 4 and spec[3]:
            nat = int(spec[3])
    elif isinstance(spec, dict):
        ip = spec.get("ip")
        port = spec.get("port")
        if spec.get("tcp_port"):
            tcp = int(spec["tcp_port"])
        if spec.get("nat_probe_port"):
            nat = int(spec["nat_probe_port"])
    if not ip or not port:
        return None
    return (ip, int(port), int(tcp), int(nat))


def _normalize_servers(servers: Any, default_ip: Optional[str],
                       default_port: Optional[int],
                       default_tcp: int,
                       default_nat: int) -> List[Tuple[str, int, int, int]]:
    """构建服务器候选列表（供多服务器自动故障转移）。

    优先使用 servers；否则退回 default_ip:default_port 单个候选。
    """
    tcp = default_tcp or C.DEFAULT_SERVER_TCP_PORT
    nat = default_nat or C.NAT_PROBE_PORT
    out = []
    if servers:
        for s in servers:
            item = _parse_server_spec(s, tcp, nat)
            if item:
                out.append(item)
    if not out:
        ip = default_ip
        port = default_port or C.DEFAULT_SERVER_PORT
        if ip:
            out.append((ip, int(port), int(tcp), int(nat)))
    return out


class SignalingClient:
    """UDP 信令客户端。

    回调：
      on_joined(members)
      on_member_join(member) / on_member_leave(peer_id)
      on_punch_go(peer, at_ms)
      on_error(code)
      on_udp_hole_ready(peer_id, peer_addr)   UDP 打洞成功
      on_mapping_ready()                       TCP 映射就绪
    """

    def __init__(self, server_ip: Optional[str] = None,
                 server_port: Optional[int] = None,
                 room: Optional[str] = None,
                 name: Optional[str] = None,
                 tcp_port: Optional[int] = None,
                 lan_ips: Any = None,
                 on_joined: Any = None,
                 on_member_join: Any = None,
                 on_member_leave: Any = None,
                 on_punch_go: Any = None,
                 on_error: Any = None,
                 log: Optional[Callable[[str], None]] = None,
                 server_tcp_port: Optional[int] = None,
                 punch_local_port: Optional[int] = None,
                 device_id: Optional[str] = None,
                 on_udp_hole_ready: Any = None,
                 on_mapping_ready: Any = None,
                 udp_hole_port: Optional[int] = None,
                 nat_probe_port: Optional[int] = None,
                 config=None,
                 servers: Any = None,
                 auto_failover: bool = True,
                 connect_timeout: float = 10.0) -> None:
        if config is None:
            from ..config import DEFAULT_CONFIG
            config = DEFAULT_CONFIG
        self.cfg = config
        self._servers = _normalize_servers(
            servers, server_ip, server_port,
            server_tcp_port or C.DEFAULT_SERVER_TCP_PORT,
            nat_probe_port or C.NAT_PROBE_PORT)
        self._using_default_server = False
        if not self._servers:
            # 未指定服务器 → 回退到默认服务器（便利入口，可被参数覆盖）
            from ..defaults import default_servers
            self._servers = default_servers()
            self._using_default_server = True
        if not self._servers:
            raise ValueError("SignalingClient 需要 server_ip 或 servers 之一")
        self._server_idx = 0
        self._auto_failover = auto_failover
        self.connect_timeout = connect_timeout
        self.server_ip = self._servers[0][0]
        self.server_port = self._servers[0][1]
        self.server_tcp_port = self._servers[0][2]
        self.room = str(room) if room is not None else ""
        self.name = name
        self.tcp_port = tcp_port
        self.lan_ips = list(lan_ips or [])
        self.device_id = device_id or ""
        self.punch_local_port = punch_local_port
        # 房间模式 UDP 打洞端口（默认 9996，可自定义以便同机多实例 / 测试）
        self.udp_hole_port = udp_hole_port or C.ROOM_UDP_PORT
        # NAT 类型探测端口（默认 3338，可自定义以匹配服务器配置）
        self.nat_probe_port = self._servers[0][3]
        # 服务器时间 - 本地时间（毫秒）
        self.clock_offset_ms = 0
        self.on_joined = on_joined
        self.on_member_join = on_member_join
        self.on_member_leave = on_member_leave
        self.on_punch_go = on_punch_go
        self.on_error = on_error
        self.on_udp_hole_ready = on_udp_hole_ready
        self.on_mapping_ready = on_mapping_ready
        self.log = log or (lambda msg: None)
        self.my_id = None
        self._joined_members = []
        self.sock = None
        self.map_sock = None
        self._running = False
        self._recv_thread = None
        self._hb_thread = None
        # UDP 打洞与可靠通道
        self.udp_hole_sock = None
        self._udp_hole_thread = None
        self._udp_rtp_handlers = {}
        self._udp_rtp_lock = threading.Lock()
        self._udp_recv_running = False
        self._udp_recv_thread = None
        self._hole_targets = {}
        self._hole_targets_lock = threading.Lock()
        # 身份复用：rebind 时保存旧 my_id，join 时带上，服务器可复用（身份稳定）
        self._reuse_id = ""

    # ---------- 生命周期 ----------

    def start(self) -> bool:
        """连接信令服务器并加入房间。

        支持多服务器自动故障转移：依次尝试 self._servers 中的每个候选，
        第一个成功即停。全部失败返回 False。
        """
        if self._running:
            return False
        # 默认服务器到期提醒（仅在使用默认服务器时）
        if self._using_default_server:
            try:
                from ..defaults import check_default_server_expiry
                st = check_default_server_expiry()
                if st["status"] == "expired":
                    self.log("[信令] ⚠ 默认服务器已于 %s 过期（%d 天前）。"
                             "请改用自建服务器或备用服务器。"
                             % (st["expires"], -st["days_left"]))
                elif st["status"] == "soon":
                    self.log("[信令] ⚠ 默认服务器将于 %s 到期（剩 %d 天），"
                             "建议尽早改用自建服务器。"
                             % (st["expires"], st["days_left"]))
            except Exception:
                pass
        self._running = True
        n = len(self._servers)
        last_err = ""
        for idx in range(n):
            (ip, port, tcp_port, nat_port) = self._servers[idx]
            self._server_idx = idx
            self.server_ip = ip
            self.server_port = port
            self.server_tcp_port = tcp_port
            self.nat_probe_port = nat_port
            if idx > 0:
                self.log("[信令] 切换备用服务器 -> %s:%d（第 %d/%d 个）"
                         % (ip, port, idx + 1, n))
            if self._try_connect_one():
                return True
            last_err = "%s:%d" % (ip, port)
            if not self._auto_failover:
                break
        # 全部失败
        self._running = False
        err = ("[信令] 无法连接任何信令服务器（共 %d 个候选，最后尝试 %s）。"
               "请检查：服务器是否已启动、地址是否正确、"
               "防火墙是否放行 UDP。" % (n, last_err))
        self.log(err)
        if self.on_error:
            try:
                self.on_error("CONNECT_FAILED")
            except Exception:
                pass
        return False

    def _try_connect_one(self) -> bool:
        """尝试连接当前 self.server_ip 并加入房间。成功返回 True。"""
        try:
            fam = socket.AF_INET6 if ":" in self.server_ip else socket.AF_INET
            self.sock = socket.socket(fam, socket.SOCK_DGRAM)
            self.sock.settimeout(self.cfg.recv_timeout)
        except Exception as e:
            self.log("[信令] 创建 socket 失败: %s" % e)
            return False
        self._send_join()
        if not self._wait_joined():
            self.log("[信令] 连接 %s:%d 失败（%.0f 秒内无响应）"
                     % (self.server_ip, self.server_port, self.connect_timeout))
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
            return False
        if self.my_id:
            self._sync_time()
            self._open_mapping()
            self._open_udp_hole_socket()
            time.sleep(0.2)
        if self.on_joined:
            self.on_joined(self._joined_members)
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._recv_thread.start()
        self._hb_thread = threading.Thread(target=self._hb_loop, daemon=True)
        self._hb_thread.start()
        self.log("[信令] 已加入房间 %s，我的ID=%s" % (self.room, self.my_id))
        threading.Thread(target=self._run_nat_probe, daemon=True).start()
        return True

    def _run_nat_probe(self) -> None:
        try:
            r = natprobe.detect_nat_type(self.server_ip, self.server_port,
                                         self.nat_probe_port, timeout=1.5,
                                         log=self.log)
            t = r.get("type")
            names = {"cone": "锥形 NAT（Cone）",
                     "symmetric": "对称 NAT（Symmetric）",
                     "no_udp": "UDP 被封堵（无法探测）",
                     "unknown": "未知（仅收到一路回应）"}
            self.log("[NAT探测] 本机 NAT 类型 = %s" % names.get(t, t))
            self.log("[NAT探测] 探测 1（UDP %d）观察到: %s"
                     % (self.server_port, r.get("primary")))
            self.log("[NAT探测] 探测 2（UDP %d）观察到: %s"
                     % (self.nat_probe_port, r.get("alt")))
            if t == "symmetric":
                self.log("[NAT探测] 提示：本机为对称 NAT，TCP 打洞成功率极低。")
            elif t == "cone":
                self.log("[NAT探测] 提示：本机为锥形 NAT，打洞可行性较高。")
        except Exception as e:
            self.log("[NAT探测] 异常: %s" % e)

    def stop(self, quiet: bool = False) -> None:
        """停止信令客户端。

        quiet=True：不发 BYE（用于网络切换重建）——让服务器保留本成员条目，
        对端不收到 member_leave，重建后复用同一 id 回归（无感重建）。
        """
        if not self._running:
            return
        self._running = False
        if not quiet:
            try:
                if self.my_id:
                    self._send({"type": C.T_BYE, "ver": C.DPMP_VER, "id": self.my_id})
            except Exception:
                pass
        try:
            if self.map_sock:
                self.map_sock.close()
        except Exception:
            pass
        self._udp_recv_running = False
        try:
            if self.udp_hole_sock:
                self.udp_hole_sock.close()
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass

    def rebind(self) -> bool:
        """网络切换后重建信令连接：重开所有 socket 并重新加入房间。

        宿主应用在检测到网络切换（Wi-Fi/蜂窝切换、IP 变化）时调用。
        · 不发 BYE（quiet）——服务器保留成员条目，对端不掉线
        · 携带旧 my_id（reuse_id）——服务器支持则复用身份，实现无感重建；
          旧服务器忽略该字段，退化为生成新 id（等价于重新 join）

        返回 True 表示重建成功。
        """
        if not self._running:
            return False
        self._reuse_id = self.my_id or ""
        self.stop(quiet=True)
        # 重置运行期状态，供 start 重新初始化（保留 _reuse_id）
        self.my_id = None
        self._joined_members = []
        self._udp_rtp_handlers.clear()
        self._hole_targets.clear()
        return self.start()

    # ---------- UDP 打洞与可靠通道 ----------

    def _open_udp_hole_socket(self) -> None:
        try:
            fam = socket.AF_INET6 if ":" in self.server_ip else socket.AF_INET
            self.udp_hole_sock = socket.socket(fam, socket.SOCK_DGRAM)
            self.udp_hole_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if fam == socket.AF_INET6:
                self.udp_hole_sock.bind(("::", self.udp_hole_port))
            else:
                self.udp_hole_sock.bind(("", self.udp_hole_port))
            self.udp_hole_sock.settimeout(1.0)
            hello = json.dumps({"type": C.T_UDP_HELLO, "ver": C.DPMP_VER,
                                "id": self.my_id}).encode("utf-8")
            self.udp_hole_sock.sendto(hello, (self.server_ip, self.server_port))
            self.log("[UDP打洞] 打洞 socket 已绑定本地 %d，已向服务器登记映射" % self.udp_hole_port)
            if not self._udp_recv_running:
                self._udp_recv_running = True
                self._udp_recv_thread = threading.Thread(
                    target=self._udp_global_recv_loop, daemon=True)
                self._udp_recv_thread.start()
            for m in self._joined_members:
                mid = m.get("id")
                if mid:
                    try:
                        self._send({"type": C.T_PUNCH_REQ, "ver": C.DPMP_VER,
                                    "id": self.my_id, "target": mid})
                    except Exception:
                        pass
        except Exception as e:
            self.log("[UDP打洞] socket 打开失败: %s" % e)
            self.udp_hole_sock = None

    def send_udp_to(self, peer_addr: Tuple[str, int], data: bytes) -> None:
        """向指定对端地址发送一个 UDP 包（UDP-RTP 发送入口）。"""
        try:
            if self.udp_hole_sock:
                self.udp_hole_sock.sendto(data, peer_addr)
        except Exception as e:
            self.log("[UDP-RTP] send_udp_to 失败: %s" % e)

    def register_udp_rtp(self, peer_key: str,
                         on_packet: Callable[[bytes], None]) -> None:
        """注册某对端地址的 RTP 处理回调。peer_key 为 'ip:port'。"""
        with self._udp_rtp_lock:
            self._udp_rtp_handlers[peer_key] = on_packet

    def unregister_udp_rtp(self, peer_key: str) -> None:
        with self._udp_rtp_lock:
            self._udp_rtp_handlers.pop(peer_key, None)

    def _udp_global_recv_loop(self) -> None:
        """全局 UDP 接收循环：按源地址分发打洞探测包与 RTP 数据包。"""
        self.log("[UDP-RTP] 全局接收循环启动")
        buf = bytearray(2048)
        while self._udp_recv_running and self.udp_hole_sock:
            try:
                self.udp_hole_sock.settimeout(0.5)
                n, addr = self.udp_hole_sock.recvfrom_into(buf, len(buf))
            except socket.timeout:
                continue
            except Exception:
                break
            if n <= 0:
                continue
            data = bytes(buf[:n])
            key = "%s:%d" % (addr[0], addr[1])
            if data == C.UDP_HOLE_MAGIC:
                with self._hole_targets_lock:
                    peer_id = self._hole_targets.get(key)
                if peer_id is not None:
                    with self._udp_rtp_lock:
                        already = key in self._udp_rtp_handlers
                    if not already:
                        self.log("[UDP打洞] ★ 成功！收到 %s 的探测包（peer=%s）"
                                 % (key, peer_id))
                        if self.on_udp_hole_ready:
                            try:
                                self.on_udp_hole_ready(peer_id, addr)
                            except Exception as ex:
                                self.log("[UDP打洞] 回调异常: %s" % ex)
                continue
            with self._udp_rtp_lock:
                handler = self._udp_rtp_handlers.get(key)
            if handler:
                try:
                    handler(data)
                except Exception as e:
                    self.log("[UDP-RTP] 处理包异常: %s" % e)
        self.log("[UDP-RTP] 全局接收循环退出")

    def _start_udp_hole(self, peer: dict) -> None:
        """收到 punch_go 后，启动 UDP 打洞探测：向对端 pub_udp 持续发包。"""
        if self.udp_hole_sock is None:
            self.log("[UDP打洞] socket 未打开，跳过")
            return
        peer_id = peer.get("id")
        pub_udp = peer.get("pub_udp", "")
        parsed = parse_host_port(pub_udp)
        if not parsed:
            self.log("[UDP打洞] 对端 pub_udp 无效（%s），无法打洞" % pub_udp)
            return
        ip, port = parsed
        key = "%s:%d" % (ip, port)
        with self._hole_targets_lock:
            self._hole_targets[key] = peer_id

        def _probe() -> None:
            t_end = time.time() + self.cfg.udp_probe_duration
            sent = 0
            self.log("[UDP打洞] 开始向 %s 发探测包（%.1f 秒）"
                     % (key, self.cfg.udp_probe_duration))
            while time.time() < t_end:
                if not self._running:
                    break
                try:
                    self.udp_hole_sock.sendto(C.UDP_HOLE_MAGIC, (ip, port))
                    sent += 1
                except Exception as e:
                    self.log("[UDP打洞] 发送失败: %s" % e)
                    break
                time.sleep(self.cfg.udp_probe_interval)
            with self._hole_targets_lock:
                still_waiting = self._hole_targets.get(key) == peer_id
            with self._udp_rtp_lock:
                has_handler = key in self._udp_rtp_handlers
            if still_waiting and not has_handler:
                with self._hole_targets_lock:
                    self._hole_targets.pop(key, None)
                self.log("[UDP打洞] 失败：发了 %d 个包，未收到 %s 响应（peer=%s）"
                         % (sent, key, peer_id))

        self._udp_hole_thread = threading.Thread(target=_probe, daemon=True)
        self._udp_hole_thread.start()

    def request_punch(self, target_id: str) -> None:
        self._send({"type": C.T_PUNCH_REQ, "ver": C.DPMP_VER,
                    "id": self.my_id, "target": target_id})

    # ---------- 信令收发 ----------

    def _send(self, obj: dict) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.sock.sendto(data, (self.server_ip, self.server_port))

    def _send_join(self) -> None:
        msg = {
            "type": C.T_JOIN, "ver": C.DPMP_VER, "room": self.room,
            "name": self.name, "tcp": self.tcp_port, "lan": self.lan_ips,
            "did": self.device_id,
        }
        # 网络重建时携带旧 id，服务器复用之（协议向后兼容：旧服务器忽略该字段）
        if self._reuse_id:
            msg["reuse_id"] = self._reuse_id
        self._send(msg)

    def _sync_time(self) -> None:
        """NTP 式时间同步（4 次采样取最小 RTT）。

        offset = t2 - (t1 + t3) / 2  （服务器时间 - 本地时间）
        """
        import time as _t
        best_rtt = None
        best_offset = 0
        old_timeout = None
        try:
            old_timeout = self.sock.gettimeout()
        except Exception:
            pass
        for _ in range(4):
            try:
                t1 = int(_t.time() * 1000)
                self._send({"type": C.T_TIME_REQ, "ver": C.DPMP_VER, "t1": t1})
                self.sock.settimeout(1.0)
                deadline = _t.time() + 1.0
                reply = None
                while _t.time() < deadline:
                    try:
                        data, _addr = self.sock.recvfrom(65535)
                    except socket.timeout:
                        break
                    try:
                        msg = json.loads(data.decode("utf-8"))
                    except Exception:
                        continue
                    if msg.get("type") == C.T_TIME_REPLY and msg.get("t1") == t1:
                        reply = msg
                        break
                if reply is None:
                    continue
                t3 = int(_t.time() * 1000)
                t2 = int(reply.get("t2", 0))
                rtt = t3 - t1
                offset = t2 - (t1 + t3) // 2
                if best_rtt is None or rtt < best_rtt:
                    best_rtt = rtt
                    best_offset = offset
                _t.sleep(0.05)
            except Exception as e:
                self.log("[校时] 采样失败: %s" % e)
                break
        try:
            if old_timeout is None:
                self.sock.settimeout(self.cfg.recv_timeout)
            else:
                self.sock.settimeout(old_timeout)
        except Exception:
            pass
        if best_rtt is not None:
            self.clock_offset_ms = best_offset
            self.log("[校时] 与服务器时钟偏差 = %d ms（最小 RTT %d ms）"
                     % (best_offset, best_rtt))
        else:
            self.log("[校时] 未能同步，使用本地时钟（可能影响打洞时刻）")

    def _open_mapping(self) -> None:
        fam = socket.AF_INET6 if ":" in self.server_ip else socket.AF_INET
        for attempt in range(1, 4):
            s = None
            try:
                s = socket.socket(fam, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if self.punch_local_port:
                    bind_addr = (("::", self.punch_local_port) if fam == socket.AF_INET6
                                 else ("", self.punch_local_port))
                    s.bind(bind_addr)
                s.connect((self.server_ip, self.server_tcp_port))
                mid = self.my_id.encode("utf-8")
                s.sendall(struct.pack("!H", len(mid)) + mid)
                self.map_sock = s
                self.log("[信令] TCP 映射观测连接已建立（本地端口=%s）" % s.getsockname()[1])
                if self.on_mapping_ready:
                    try:
                        self.on_mapping_ready()
                    except Exception as e:
                        self.log("[信令] on_mapping_ready 回调异常: %s" % e)
                return
            except Exception as e:
                if s:
                    try:
                        s.close()
                    except Exception:
                        pass
                if attempt < 3:
                    time.sleep(0.5)
                    continue
                self.log("[信令] TCP 映射观测连接失败: %s" % e)

    def _wait_joined(self) -> bool:
        deadline = time.time() + self.connect_timeout
        while time.time() < deadline and self._running:
            try:
                data, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                self._send_join()
                continue
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            t = msg.get("type")
            if t == C.T_JOINED:
                self.my_id = msg.get("id")
                self._joined_members = msg.get("members", [])
                return True
            elif t == C.T_ERROR:
                if self.on_error:
                    self.on_error(msg.get("code", "UNKNOWN"))
                self._running = False
                return False
        return False

    def _recv_loop(self) -> None:
        while self._running:
            try:
                data, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception:
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            t = msg.get("type")
            if t == C.T_MEMBER_JOIN:
                if self.on_member_join:
                    self.on_member_join(msg.get("member", {}))
            elif t == C.T_MEMBER_LEAVE:
                if self.on_member_leave:
                    self.on_member_leave(msg.get("id"))
            elif t == C.T_PUNCH_GO:
                peer = msg.get("peer", {})
                try:
                    self._start_udp_hole(peer)
                except Exception as e:
                    self.log("[UDP打洞] 启动异常: %s" % e)
                if self.on_punch_go:
                    self.on_punch_go(peer, msg.get("at", 0))
            elif t == C.T_ERROR:
                if self.on_error:
                    self.on_error(msg.get("code", "UNKNOWN"))

    def _hb_loop(self) -> None:
        while self._running:
            time.sleep(self.cfg.heartbeat_interval)
            if not self._running:
                break
            try:
                self._send({"type": C.T_HB, "ver": C.DPMP_VER, "id": self.my_id})
            except Exception:
                pass
