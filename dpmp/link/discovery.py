# -*- coding: utf-8 -*-
"""局域网设备发现：UDP 广播 + 主动扫描 + 心跳。

节点按 device_id 去重（IP 变化不产生重复条目）。发现完全依赖
「请求 → 响应」：收到广播/扫描请求 → 回 reply；收到 reply → 登记节点。

端口：
  · 9998 UDP  广播 / 心跳
  · 9997 UDP  主动扫描（与收发隔离，避免扫描影响传输）
"""

import json
import socket
import threading
import time

from ..protocol import constants as C
from ..util.net import (get_all_local_ips, get_broadcast_addrs,
                        get_subnet_for_ip, get_mac_address)


class Node:
    """一个已发现的对端设备（dict 形式存储于 Discovery.nodes）。"""


class Discovery:
    """局域网设备发现。

    参数：
      device_id：本机稳定标识（用于去重，不显示自己）
      hostname：本机显示名
      log：日志回调
      on_new_node(ip, msg)：发现新节点时回调
      on_node_gone(ip)：节点下线时回调

    对外：
      nodes      {ip: node_dict}
      start() / stop()
      broadcast_search()        广播搜索
      scan_subnet(cidr)         扫描子网
      add_manual_node(ip)       手动添加
    """

    def __init__(self, device_id, hostname, log=None,
                 on_new_node=None, on_node_gone=None,
                 udp_port=None, scan_port=None, config=None):
        if config is None:
            from ..config import DEFAULT_CONFIG
            config = DEFAULT_CONFIG
        self.cfg = config
        self.discover_interval = config.discover_interval
        self.device_id = device_id or ""
        self.hostname = hostname or socket.gethostname()
        self.mac = get_mac_address()
        self.log = log or (lambda m: None)
        self.on_new_node = on_new_node
        self.on_node_gone = on_node_gone
        # 局域网端口（默认 9998 发现 / 9997 扫描，可自定义）
        self.udp_port = udp_port or C.LAN_UDP_PORT
        self.scan_port = scan_port or C.SCAN_PORT

        self.my_ips = get_all_local_ips(ipv6=False)
        self.my_ips_v6 = get_all_local_ips(ipv6=True)
        self.broadcast_addrs = get_broadcast_addrs()

        self.nodes = {}
        self.lock = threading.Lock()
        self.running = False
        self.broadcast_sockets = []
        self.listen_sockets = []
        self.auto_scan_enabled = False
        self.scanning = False
        self.pausing_network = False   # 传输时暂停广播/扫描/心跳

    # ---------- 生命周期 ----------

    def start(self):
        self.running = True
        threading.Thread(target=self._udp_listener, daemon=True).start()
        threading.Thread(target=self._scan_listener, daemon=True).start()
        self._start_broadcasters()
        self.log("本机 IPv4: %s" % ", ".join(self.my_ips))
        if self.my_ips_v6:
            self.log("本机 IPv6: %s" % ", ".join(self.my_ips_v6))
        self.log("广播地址: %s" % ", ".join(self.broadcast_addrs))

    def stop(self):
        self.running = False
        for s in self.broadcast_sockets + self.listen_sockets:
            try:
                s.close()
            except Exception:
                pass
        self._send_bye()

    # ---------- 消息构造 ----------

    def _build_msg(self, **extra):
        msg = {
            C.LAN_F_HOSTNAME: self.hostname,
            C.LAN_F_DEVICE_ID: self.device_id,
            C.LAN_F_MAC: self.mac,
        }
        msg.update(extra)
        return json.dumps(msg).encode("utf-8")

    def _upsert_node(self, remote_ip, msg, source="udp"):
        """按 device_id 去重地插入/更新节点。返回 True 表示是新节点。"""
        device_id = msg.get(C.LAN_F_DEVICE_ID, "") or ""
        hostname = msg.get(C.LAN_F_HOSTNAME, remote_ip) or remote_ip
        mac = msg.get(C.LAN_F_MAC, "") or ""
        is_new = False
        with self.lock:
            if device_id:
                for old_ip in list(self.nodes.keys()):
                    if old_ip == remote_ip:
                        continue
                    if self.nodes[old_ip].get(C.LAN_F_DEVICE_ID) == device_id:
                        del self.nodes[old_ip]
            if remote_ip in self.nodes:
                self.nodes[remote_ip][C.LAN_F_HOSTNAME] = hostname
                self.nodes[remote_ip][C.LAN_F_DEVICE_ID] = device_id
                self.nodes[remote_ip][C.LAN_F_MAC] = mac
                self.nodes[remote_ip]["last_seen"] = time.time()
                self.nodes[remote_ip]["heartbeat_fail"] = 0
            else:
                self.nodes[remote_ip] = {
                    C.LAN_F_HOSTNAME: hostname,
                    C.LAN_F_DEVICE_ID: device_id,
                    C.LAN_F_MAC: mac,
                    "last_seen": time.time(),
                    "source": source,
                    "heartbeat_fail": 0,
                }
                is_new = True
        return is_new

    def get_nodes(self):
        with self.lock:
            return dict(self.nodes)

    # ---------- 广播 ----------

    def _start_broadcasters(self):
        for ip_str in self.my_ips:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((ip_str, 0))
                self.broadcast_sockets.append(sock)
                threading.Thread(target=self._udp_broadcaster_on_sock,
                                 args=(sock, ip_str), daemon=True).start()
            except Exception as e:
                self.log("[警告] 无法为 %s 创建广播 socket: %s" % (ip_str, e))
        try:
            global_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            global_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            global_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.broadcast_sockets.append(global_sock)
            threading.Thread(target=self._udp_broadcaster_on_sock,
                             args=(global_sock, None), daemon=True).start()
        except Exception as e:
            self.log("[警告] 全局广播 socket 创建失败: %s" % e)

    def _udp_broadcaster_on_sock(self, sock, bind_ip):
        """周期性广播搜索：前 5 秒每 0.2s，之后每 20s。"""
        start = time.time()
        while self.running:
            if not self.pausing_network:
                try:
                    sock.sendto(self._build_msg(**{C.LAN_K_DISCOVERY: True}),
                                ("255.255.255.255", self.udp_port))
                except Exception:
                    pass
            elapsed = time.time() - start
            if elapsed < 5:
                time.sleep(0.2)
            else:
                time.sleep(self.discover_interval)

    def broadcast_search(self):
        """广播搜索：向所有广播地址爆发式发送探测包，等待设备回复。"""
        self.log("[广播搜索] 发送广播探测，等待设备回复...")
        msg = self._build_msg(**{C.LAN_K_DISCOVERY: True})
        for _ in range(3):
            for bcast_addr in self.broadcast_addrs:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    s.settimeout(1)
                    s.sendto(msg, (bcast_addr, self.udp_port))
                    s.close()
                except Exception:
                    pass
            time.sleep(0.2)
        self.log("[广播搜索] 完成")

    # ---------- 接收 ----------

    def _udp_listener(self):
        sock4 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock4.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock4.bind(("", self.udp_port))
        sock4.settimeout(1.0)
        self.listen_sockets.append(sock4)
        try:
            sock6 = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            sock6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock6.bind(("::", self.udp_port))
            sock6.settimeout(1.0)
            self.listen_sockets.append(sock6)
        except Exception:
            sock6 = None

        def listen_sock(sock, is_ipv6=False):
            while self.running:
                try:
                    data, addr = sock.recvfrom(1024)
                    msg = json.loads(data.decode("utf-8"))
                    remote_ip = addr[0]
                    if remote_ip in self.my_ips or remote_ip in self.my_ips_v6:
                        continue
                    if remote_ip in ("127.0.0.1", "::1", "0.0.0.0"):
                        continue
                    if msg.get(C.LAN_F_DEVICE_ID) and msg.get(C.LAN_F_DEVICE_ID) == self.device_id:
                        continue
                    if msg.get(C.LAN_K_BYE):
                        with self.lock:
                            removed = remote_ip in self.nodes
                            self.nodes.pop(remote_ip, None)
                        if removed and self.on_node_gone:
                            try:
                                self.on_node_gone(remote_ip)
                            except Exception:
                                pass
                        continue
                    if msg.get(C.LAN_K_HEARTBEAT):
                        with self.lock:
                            if remote_ip in self.nodes:
                                self.nodes[remote_ip]["last_seen"] = time.time()
                        reply = self._build_msg(**{C.LAN_K_HEARTBEAT: True, C.LAN_K_ACK: True})
                        sock.sendto(reply, (remote_ip, self.udp_port))
                        continue
                    is_new = self._upsert_node(remote_ip, msg, source="udp")
                    if is_new:
                        hn = msg.get(C.LAN_F_HOSTNAME, remote_ip)
                        self.log("[发现] 新设备 %s (%s)" % (hn, remote_ip))
                        if self.on_new_node:
                            try:
                                self.on_new_node(remote_ip, msg)
                            except Exception:
                                pass
                    if not msg.get(C.LAN_K_REPLY):
                        sock.sendto(self._build_msg(**{C.LAN_K_REPLY: True}),
                                    (remote_ip, self.udp_port))
                except socket.timeout:
                    continue
                except OSError:
                    break
                except Exception:
                    pass
            try:
                sock.close()
            except Exception:
                pass

        threading.Thread(target=listen_sock, args=(sock4, False), daemon=True).start()
        if sock6:
            threading.Thread(target=listen_sock, args=(sock6, True), daemon=True).start()

    # ---------- 扫描 ----------

    def _scan_listener(self):
        """扫描端口监听：收到扫描探测则回 scan_reply。"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", self.scan_port))
            sock.settimeout(1.0)
            self.listen_sockets.append(sock)
        except Exception as e:
            self.log("[扫描] 监听失败: %s" % e)
            return
        while self.running:
            try:
                data, addr = sock.recvfrom(1024)
                msg = json.loads(data.decode("utf-8"))
                remote_ip = addr[0]
                if msg.get(C.LAN_F_DEVICE_ID) and msg.get(C.LAN_F_DEVICE_ID) == self.device_id:
                    continue
                if remote_ip in self.my_ips or remote_ip in ("127.0.0.1", "::1"):
                    continue
                sock.sendto(self._build_msg(**{C.LAN_K_SCAN_REPLY: True}),
                            (remote_ip, self.scan_port))
                is_new = self._upsert_node(remote_ip, msg, source="scan")
                if is_new and self.on_new_node:
                    try:
                        self.on_new_node(remote_ip, msg)
                    except Exception:
                        pass
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception:
                pass
        try:
            sock.close()
        except Exception:
            pass

    def scan_subnet(self, network_cidr, callback=None):
        """扫描一个子网（IPv4），发现的对端通过 on_new_node 回调。"""
        import ipaddress
        try:
            net = ipaddress.IPv4Network(network_cidr, strict=False)
        except Exception:
            return
        self.scanning = True
        msg = self._build_msg(**{C.LAN_K_SCAN_REPLY: True})
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(0.3)
            for host in net.hosts():
                if not self.running:
                    break
                try:
                    sock.sendto(msg, (str(host), self.scan_port))
                except Exception:
                    pass
        except Exception as e:
            self.log("[扫描] 子网 %s 扫描异常: %s" % (network_cidr, e))
        finally:
            try:
                sock.close()
            except Exception:
                pass
            self.scanning = False

    def add_manual_node(self, ip, hostname=None):
        """手动添加节点，并主动发探测让对方也发现我们。"""
        with self.lock:
            self.nodes[ip] = {
                "hostname": hostname or ip,
                "last_seen": time.time(),
                "source": "manual",
                "heartbeat_fail": 0,
            }
        self._send_probe_to(ip)
        self.log("[手动添加] 已向 %s 发送探测" % ip)

    def _send_probe_to(self, ip):
        try:
            family = socket.AF_INET6 if ":" in ip else socket.AF_INET
            s = socket.socket(family, socket.SOCK_DGRAM)
            s.settimeout(2)
            s.sendto(self._build_msg(), (ip, self.udp_port))
            try:
                s.sendto(self._build_msg(**{C.LAN_K_SCAN_REPLY: True}), (ip, self.scan_port))
            except Exception:
                pass
            s.close()
        except Exception as e:
            self.log("[手动添加] 向 %s 发送探测失败: %s" % (ip, e))

    # ---------- 下线 ----------

    def _send_bye(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            s.sendto(self._build_msg(**{C.LAN_K_BYE: True}), ("255.255.255.255", self.udp_port))
            s.close()
        except Exception:
            pass
