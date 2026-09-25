# -*- coding: utf-8 -*-
"""
信令服务器 —— 公网房间匹配与打洞协调

职责（单一）：
  1. 设备按“房间号”注册上线，服务器维护房间成员表
  2. 记录每个设备的 TCP 公网映射（供 TCP 打洞使用）
  3. 向新成员下发房间现有成员地址，向老成员广播新成员加入
  4. 协调两成员同时打洞（下发统一打洞时刻 + 对方 TCP 映射）
  5. 心跳保活，超时成员剔除并广播离开

本服务器不转发任何文件数据，仅处理控制小包。
"""

import json
import os
import socket
import struct
import sys
import threading
import time
import uuid

# Windows 终端默认 GBK，中文日志会乱码；强制 stdout/stderr 用 UTF-8。
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ==================== 默认配置 ====================
# 可通过以下方式覆盖（优先级从低到高）：
#   1. 本文件的默认值
#   2. 同目录下的 config.json（或 --config 指定的文件）
#   3. 命令行参数（--port 等）
DEFAULTS = {
    "VER": 1,                  # 协议版本
    "LISTEN_IP": "0.0.0.0",    # 监听地址
    "LISTEN_PORT": 3336,       # UDP 信令端口
    "TCP_LISTEN_PORT": 3337,   # TCP 映射观测端口
    "NAT_PROBE_PORT": 3338,    # UDP NAT 探测端口（与 3336 不同，用于对称 NAT 检测）
    "MAX_ROOM_SIZE": 16,       # 单房间最大人数
    "HEARTBEAT_TIMEOUT": 30,   # 成员心跳超时（秒）
    "CLEAN_INTERVAL": 5,       # 清理线程扫描间隔（秒）
    "PUNCH_DELAY_MS": 1000,    # 下发 punch_go 后延迟（毫秒）
                               # 需覆盖：服务器→两端 UDP 下发延迟 + 客户端处理
                               # 太小会导致快的一端已超时、慢的一端刚开始打洞
    "MAX_PACKET": 65535,       # 单包接收缓冲
    "STATE_FILE": "server_state.json",  # 房间状态持久化文件（跨重启保持房间）
    "STATE_SAVE_INTERVAL": 5,  # 状态自动保存间隔（秒）
}

# 全局配置（由 load_config() 填充，单一来源）
VER = DEFAULTS["VER"]
LISTEN_IP = DEFAULTS["LISTEN_IP"]
LISTEN_PORT = DEFAULTS["LISTEN_PORT"]
TCP_LISTEN_PORT = DEFAULTS["TCP_LISTEN_PORT"]
NAT_PROBE_PORT = DEFAULTS["NAT_PROBE_PORT"]
MAX_ROOM_SIZE = DEFAULTS["MAX_ROOM_SIZE"]
HEARTBEAT_TIMEOUT = DEFAULTS["HEARTBEAT_TIMEOUT"]
CLEAN_INTERVAL = DEFAULTS["CLEAN_INTERVAL"]
PUNCH_DELAY_MS = DEFAULTS["PUNCH_DELAY_MS"]
MAX_PACKET = DEFAULTS["MAX_PACKET"]
STATE_FILE = DEFAULTS["STATE_FILE"]
STATE_SAVE_INTERVAL = DEFAULTS["STATE_SAVE_INTERVAL"]

# 状态文件绝对路径（main 时按脚本目录解析）
_state_path = None

# ==================== 配置加载 ====================
def _apply_config(cfg):
    """把配置字典写入全局变量（单一来源）。"""
    global VER, LISTEN_IP, LISTEN_PORT, TCP_LISTEN_PORT, NAT_PROBE_PORT, MAX_ROOM_SIZE
    global HEARTBEAT_TIMEOUT, CLEAN_INTERVAL, PUNCH_DELAY_MS, MAX_PACKET
    global STATE_FILE, STATE_SAVE_INTERVAL
    VER = int(cfg.get("VER", VER))
    LISTEN_IP = str(cfg.get("LISTEN_IP", LISTEN_IP))
    LISTEN_PORT = int(cfg.get("LISTEN_PORT", LISTEN_PORT))
    TCP_LISTEN_PORT = int(cfg.get("TCP_LISTEN_PORT", TCP_LISTEN_PORT))
    NAT_PROBE_PORT = int(cfg.get("NAT_PROBE_PORT", NAT_PROBE_PORT))
    MAX_ROOM_SIZE = int(cfg.get("MAX_ROOM_SIZE", MAX_ROOM_SIZE))
    HEARTBEAT_TIMEOUT = int(cfg.get("HEARTBEAT_TIMEOUT", HEARTBEAT_TIMEOUT))
    CLEAN_INTERVAL = int(cfg.get("CLEAN_INTERVAL", CLEAN_INTERVAL))
    PUNCH_DELAY_MS = int(cfg.get("PUNCH_DELAY_MS", PUNCH_DELAY_MS))
    MAX_PACKET = int(cfg.get("MAX_PACKET", MAX_PACKET))
    STATE_FILE = str(cfg.get("STATE_FILE", STATE_FILE))
    STATE_SAVE_INTERVAL = int(cfg.get("STATE_SAVE_INTERVAL", STATE_SAVE_INTERVAL))


def load_config(argv=None):
    """按优先级加载配置：默认值 < config.json < 命令行参数。"""
    argv = argv if argv is not None else sys.argv[1:]

    # 1) config.json（默认同目录，或 --config 指定）
    cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    for i, a in enumerate(argv):
        if a == "--config" and i + 1 < len(argv):
            cfg_path = argv[i + 1]
        elif a.startswith("--config="):
            cfg_path = a.split("=", 1)[1]
    if os.path.isfile(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                _apply_config(json.load(f))
            print("[配置] 已加载 %s" % cfg_path)
        except Exception as e:
            print("[配置] 读取 %s 失败: %s" % (cfg_path, e))

    # 2) 命令行参数覆盖
    kv = {}
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--config":
            i += 2
            continue
        if a.startswith("--") and "=" in a:
            k, v = a[2:].split("=", 1)
            kv[k] = v
            i += 1
        elif a.startswith("--") and i + 1 < len(argv):
            kv[a[2:]] = argv[i + 1]
            i += 2
        else:
            i += 1
    # 参数名 -> 配置键（支持小写别名）
    alias = {
        "ip": "LISTEN_IP", "port": "LISTEN_PORT",
        "tcp-port": "TCP_LISTEN_PORT", "max-room": "MAX_ROOM_SIZE",
        "heartbeat-timeout": "HEARTBEAT_TIMEOUT",
        "clean-interval": "CLEAN_INTERVAL", "punch-delay": "PUNCH_DELAY_MS",
    }
    for k, v in kv.items():
        key = alias.get(k, k)
        if key in DEFAULTS or key in ("LISTEN_IP", "LISTEN_PORT", "TCP_LISTEN_PORT",
                                      "MAX_ROOM_SIZE", "HEARTBEAT_TIMEOUT",
                                      "CLEAN_INTERVAL", "PUNCH_DELAY_MS"):
            _apply_config({key: v})


# ==================== 状态持久化 ====================
def _resolve_state_path():
    """状态文件绝对路径（同脚本目录，或配置的绝对路径）。"""
    global _state_path
    if os.path.isabs(STATE_FILE):
        _state_path = STATE_FILE
    else:
        _state_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), STATE_FILE)
    return _state_path


def save_state():
    """把房间成员表持久化到磁盘（供崩溃重启后恢复）。

    只持久化"房间 + 成员标识"（名称/did/tcp/lan/pub），不持久化 TCP 映射连接
    （连接绑定在 socket 上，重启即失效，需客户端重新建立）。
    """
    try:
        path = _state_path or _resolve_state_path()
        with state_lock:
            data = {"rooms": {}, "id_index": {}}
            for rname, members in rooms.items():
                data["rooms"][rname] = {}
                for mid, m in members.items():
                    data["rooms"][rname][mid] = {
                        "name": m.get("name", ""),
                        "pub": list(m.get("pub", ("", 0))),
                        "lan": m.get("lan", []),
                        "tcp": m.get("tcp", 0),
                        "did": m.get("did", ""),
                    }
            for mid, (rname, addr) in id_index.items():
                data["id_index"][mid] = [rname, list(addr)]
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)  # 原子替换，避免写一半崩溃损坏
    except Exception as e:
        print("[状态] 保存失败: %s" % e)


def load_state():
    """启动时恢复上次的房间成员表。恢复后重置心跳时间，给客户端一个宽限期。"""
    path = _resolve_state_path()
    if not os.path.isfile(path):
        print("[状态] 无历史状态，全新启动")
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print("[状态] 读取 %s 失败: %s" % (path, e))
        return
    now = now_ms()
    with state_lock:
        for rname, members in (data.get("rooms") or {}).items():
            rooms[rname] = {}
            for mid, m in members.items():
                rooms[rname][mid] = {
                    "name": m.get("name", ""),
                    "pub": tuple(m.get("pub", ("", 0))),
                    "lan": m.get("lan", []),
                    "tcp": m.get("tcp", 0),
                    "did": m.get("did", ""),
                    "last_hb": now,  # 宽限：重置为当前，避免启动即被清理
                }
        for mid, v in (data.get("id_index") or {}).items():
            if isinstance(v, list) and len(v) == 2:
                id_index[mid] = (v[0], tuple(v[1]))
    total = sum(len(m) for m in rooms.values())
    print("[状态] 已恢复 %d 个房间、%d 个成员（等待客户端心跳重新确认）" % (len(rooms), total))


# ==================== 全局状态（所有处理函数共用） ====================
rooms = {}          # {room_name: {member_id: member_dict}}
id_index = {}       # {member_id: (room_name, addr_tuple)}
tcp_mappings = {}   # {member_id: (ip, port)}  服务器观测到的客户端 TCP 公网映射
tcp_socks = {}      # {member_id: socket}  保持 TCP 映射存活的连接
udp_mappings = {}   # {member_id: (ip, port)}  客户端 UDP 打洞 socket 的公网映射
state_lock = threading.Lock()

# ==================== 协议：消息类型 ====================
T_JOIN = "join"
T_HB = "hb"
T_PUNCH_REQ = "punch_req"
T_BYE = "bye"
T_JOINED = "joined"
T_MEMBER_JOIN = "member_join"
T_MEMBER_LEAVE = "member_leave"
T_PUNCH_GO = "punch_go"
T_ERROR = "error"
T_NAT_PROBE = "nat_probe"                 # 客户端 -> 服务器：请求回复观察到的公网地址
T_NAT_PROBE_REPLY = "nat_probe_reply"     # 服务器 -> 客户端：观察到的公网地址
T_TIME_REQ = "time_req"                   # 客户端 -> 服务器：NTP 式时间同步请求
T_TIME_REPLY = "time_reply"               # 服务器 -> 客户端：回显 t1 + 服务器时刻 t2
T_UDP_HELLO = "udp_hello"                 # 客户端 -> 服务器：登记 UDP 打洞 socket 的公网映射


# ==================== 工具函数 ====================
def get_lan_ips():
    """获取本机所有局域网 IPv4 地址（尽力而为，失败返回空列表）。"""
    ips = set()
    # 1) 通过连接外部地址探测默认出口 IP（UDP 不实际发包）
    for target in (("8.8.8.8", 80), ("1.1.1.1", 80), ("223.5.5.5", 80)):
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0.5)
            s.connect(target)
            ips.add(s.getsockname()[0])
            break
        except Exception:
            pass
        finally:
            if s:
                try:
                    s.close()
                except Exception:
                    pass
    # 2) 通过主机名解析补充
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip and not ip.startswith("127."):
                ips.add(ip)
    except Exception:
        pass
    return sorted(ips)


def get_public_ip(timeout=3):
    """尝试获取本机公网 IP；失败返回 None（不抛异常、不阻塞退出）。

    依次尝试多个公共源，任一成功即返回。
    """
    import urllib.request
    sources = (
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
        "https://ipinfo.io/ip",
    )
    for url in sources:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/7.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                ip = resp.read(64).decode("utf-8", "ignore").strip()
                if ip and len(ip) <= 45 and " " not in ip:
                    return ip
        except Exception:
            continue
    return None


def now_ms():
    return int(time.time() * 1000)


def gen_id():
    return uuid.uuid4().hex[:8]


def make_member(name, pub_addr, lan, tcp, last_hb, did=""):
    return {"name": name, "pub": pub_addr, "lan": lan, "tcp": tcp,
            "last_hb": last_hb, "did": did}


def _calc_tcp_udp_offset(member_id):
    """估算本机 NAT 的 TCP / UDP 端口分配偏移（TCP 端口 - UDP 端口）。

    依据：服务器同时观测到该客户端对【同一服务器 IP】的：
      · UDP 映射端口（udp_hello 的 recvfrom 源端口）
      · TCP 映射端口（TCP 映射观测端口 3337 accept 的源端口）
    若两者目标 IP 一致，其端口差可作为「该 NAT 对同一目标 IP 分配
    TCP/UDP 端口」的偏移估计，供对端预测 TCP 打洞候选端口。

    不可得时返回 None（对端退化为 ±2 盲猜，行为与旧版一致）。
    """
    u = udp_mappings.get(member_id)
    t = tcp_mappings.get(member_id)
    if not u or not t:
        return None
    if u[0] != t[0]:        # 目标 IP 不一致，偏移不可比
        return None
    return int(t[1]) - int(u[1])


def member_for_peer(member_id, member):
    """转成发给其他成员的精简结构，含 TCP / UDP 公网映射（IPv6 用方括号）。"""
    pub_tcp = tcp_mappings.get(member_id)
    pub_udp = udp_mappings.get(member_id)
    return {
        "id": member_id,
        "did": member.get("did", ""),
        "name": member["name"],
        "pub": fmt_addr(member["pub"]),
        "pub_tcp": fmt_addr(pub_tcp) if pub_tcp else "",
        "pub_udp": fmt_addr(pub_udp) if pub_udp else "",
        "tcp_udp_offset": _calc_tcp_udp_offset(member_id),
        "lan": member["lan"],
        "tcp": member["tcp"],
    }


def fmt_addr(addr):
    """把 (ip, port) 格式化为 'ip:port'；IPv6 用 '[ip]:port' 以便区分端口。"""
    ip, port = addr[0], addr[1]
    if ":" in ip:
        return "[%s]:%d" % (ip, port)
    return "%s:%d" % (ip, port)


# 双栈 UDP socket（IPv4 / IPv6），由 main() 初始化
_udp4 = None
_udp6 = None


def send_to(addr, obj):
    """按目标地址族自动选择 UDP socket 发送。"""
    try:
        ip = addr[0]
        s = _udp6 if ":" in ip else _udp4
        if s is None:
            return
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        s.sendto(data, addr)
    except Exception as e:
        print("[发送失败] %s -> %s" % (addr, e))


# ==================== 业务处理 ====================
def _print_rooms_overview():
    """打印当前所有在线房间总览（供运维观察）。"""
    with state_lock:
        if not rooms:
            print("    └─ 当前无在线房间")
            return
        for rname, members in rooms.items():
            names = ", ".join("%s(%s)" % (m.get("name", "?"), mid)
                              for mid, m in members.items())
            print("    └─ 房间 '%s': %d/%d 人 [%s]" % (rname, len(members), MAX_ROOM_SIZE, names))


def handle_join(sock, msg, addr):
    room = str(msg.get("room", "")).strip()
    if not room:
        send_to(addr, {"type": T_ERROR, "ver": VER, "code": "NO_ROOM"})
        return

    is_new_room = False
    removed_dups = []   # 同一设备旧 id（网络重建遗留），需广播离开
    with state_lock:
        if room not in rooms:
            rooms[room] = {}
            is_new_room = True
        members = rooms[room]

        my_did = msg.get("did", "")
        reuse_id = str(msg.get("reuse_id", "") or "").strip()

        # 决定 my_id（身份稳定 / rebind）：
        #   · 带 reuse_id 且 did 匹配（或该 id 已不在）→ 复用它。
        #     网络切换重建时复用旧 id，对端看到的是"同一成员回归"，
        #     不产生掉线感，按 peer_id 缓存的状态也不会孤儿化。
        #   · 否则生成新 id。
        my_id = None
        if reuse_id and len(reuse_id) <= 64:
            old = members.get(reuse_id)
            if old is None or (my_did and old.get("did") == my_did):
                my_id = reuse_id

        # 同 did 去重：移除该设备的【其他】旧 id（reuse_id 自身除外）。
        # 兜底防御：万一客户端没带 reuse_id 而生成新 id，也不会残留"两个自己"。
        if my_did:
            for dup_id in [m for m, info in members.items()
                           if info.get("did") == my_did and m != my_id]:
                members.pop(dup_id, None)
                id_index.pop(dup_id, None)
                s = tcp_socks.pop(dup_id, None)
                if s:
                    try:
                        s.close()
                    except Exception:
                        pass
                tcp_mappings.pop(dup_id, None)
                udp_mappings.pop(dup_id, None)
                removed_dups.append(dup_id)

        if my_id is None:
            my_id = gen_id()
        else:
            # 复用 id：换了网络，旧 TCP 映射 socket / NAT 映射全部失效，清掉重建
            s = tcp_socks.pop(my_id, None)
            if s:
                try:
                    s.close()
                except Exception:
                    pass
            tcp_mappings.pop(my_id, None)
            udp_mappings.pop(my_id, None)

        if len(members) >= MAX_ROOM_SIZE and my_id not in members:
            send_to(addr, {"type": T_ERROR, "ver": VER, "code": "ROOM_FULL"})
            print("[房间] 拒绝加入：房间 %s 已满（%d 人）" % (room, len(members)))
            return
        member = make_member(
            name=msg.get("name", "unknown"),
            pub_addr=addr,
            lan=msg.get("lan", []),
            tcp=msg.get("tcp", 0),
            last_hb=now_ms(),
            did=my_did,
        )
        members[my_id] = member
        id_index[my_id] = (room, addr)
        existing = [member_for_peer(mid, m) for mid, m in members.items() if mid != my_id]
        targets = [(mid, m) for mid, m in members.items() if mid != my_id]

    send_to(addr, {"type": T_JOINED, "ver": VER, "id": my_id, "members": existing})

    # 通知其他成员：被去重移除的旧 id 已离开（避免它们继续对旧 id 打洞）
    for dup_id in removed_dups:
        for _, m in targets:
            send_to(m["pub"], {"type": T_MEMBER_LEAVE, "ver": VER, "id": dup_id})
        print("[房间] 去重：同设备旧成员 %s 已移除" % dup_id)

    for mid, m in targets:
        send_to(m["pub"], {
            "type": T_MEMBER_JOIN, "ver": VER,
            "member": member_for_peer(my_id, member),
        })
    if is_new_room:
        print("[房间] ★ 创建房间 '%s'（首个成员：%s/%s）" % (room, msg.get("name", ""), my_id))
    else:
        print("[房间] → 加入 '%s'（%s，ID=%s）  在线: %d/%d 人"
              % (room, msg.get("name", ""), my_id, len(members), MAX_ROOM_SIZE))
    _print_rooms_overview()
    save_state()


def handle_hb(sock, msg, addr):
    mid = msg.get("id")
    with state_lock:
        info = id_index.get(mid)
        if not info:
            return
        room, _ = info
        member = rooms.get(room, {}).get(mid)
        if member:
            member["last_hb"] = now_ms()
            member["pub"] = addr


def handle_punch_req(sock, msg, addr):
    me = msg.get("id")
    target = msg.get("target")
    with state_lock:
        a = id_index.get(me)
        b = id_index.get(target)
        if not a or not b:
            return
        room_a, _ = a
        room_b, _ = b
        if room_a != room_b:
            return
        member_a = rooms[room_a].get(me)
        member_b = rooms[room_b].get(target)
        if not member_a or not member_b:
            return
        peer_a = member_for_peer(me, member_a)
        peer_b = member_for_peer(target, member_b)
        addr_a = member_a["pub"]
        addr_b = member_b["pub"]

    at = now_ms() + PUNCH_DELAY_MS
    send_to(addr_a, {"type": T_PUNCH_GO, "ver": VER, "peer": peer_b, "at": at})
    send_to(addr_b, {"type": T_PUNCH_GO, "ver": VER, "peer": peer_a, "at": at})
    print("[打洞] 房间 '%s' 协调 %s ↔ %s  at=%d（%dms 后）"
          % (room_a, member_a.get("name", me), member_b.get("name", target),
             at, PUNCH_DELAY_MS))
    print("       %s 看到的 peer: pub_tcp=%s pub_udp=%s lan=%s"
          % (member_a.get("name", me), peer_b.get("pub_tcp"),
             peer_b.get("pub_udp"), peer_b.get("lan")))
    print("       %s 看到的 peer: pub_tcp=%s pub_udp=%s lan=%s"
          % (member_b.get("name", target), peer_a.get("pub_tcp"),
             peer_a.get("pub_udp"), peer_a.get("lan")))
    # TCP/UDP 端口分配偏移诊断：offset = TCP端口 - UDP端口（同一服务器 IP）
    off_a = peer_a.get("tcp_udp_offset")
    off_b = peer_b.get("tcp_udp_offset")
    print("       [offset] %s -> %s | %s -> %s"
          % (member_a.get("name", me),
             ("%+d" % off_a) if off_a is not None else "N/A",
             member_b.get("name", target),
             ("%+d" % off_b) if off_b is not None else "N/A"))


def handle_bye(sock, msg, addr):
    remove_member(sock, msg.get("id"))


def remove_member(sock, mid):
    with state_lock:
        info = id_index.pop(mid, None)
        if not info:
            return
        room, _ = info
        member = rooms.get(room, {}).pop(mid, None)
        room_destroyed = not rooms.get(room)
        if room_destroyed:
            rooms.pop(room, None)
        others = [(oid, m) for oid, m in rooms.get(room, {}).items()]
        remaining = len(others)
        s = tcp_socks.pop(mid, None)
        tcp_mappings.pop(mid, None)
        udp_mappings.pop(mid, None)
    if s:
        try:
            s.close()
        except Exception:
            pass
    if member is None:
        return
    for oid, m in others:
        send_to(m["pub"], {"type": T_MEMBER_LEAVE, "ver": VER, "id": mid})
    name = member.get("name", "")
    if room_destroyed:
        print("[房间] ✗ 销毁房间 '%s'（最后成员 %s/%s 离开，房间已空）" % (room, name, mid))
    else:
        print("[房间] ← 离开 '%s'（%s，ID=%s）  剩余: %d 人" % (room, name, mid, remaining))
    _print_rooms_overview()
    save_state()


def handle_nat_probe(sock, msg, addr):
    """NAT 探测应答：把 recvfrom 观察到的客户端公网地址原样回给客户端。

    客户端分别向 UDP_LISTEN_PORT(3336) 和 NAT_PROBE_PORT(3338) 各发一次探测，
    通过比较两次观察到的公网端口是否相同来判定 NAT 类型：
      · 相同  -> 锥形 NAT（Cone）
      · 不同  -> 对称 NAT（Symmetric）
      · 都无响应 -> 无法判定（可能 UDP 被封）
    """
    send_to(addr, {
        "type": T_NAT_PROBE_REPLY,
        "ver": VER,
        "probe": msg.get("probe", ""),     # 回显探测标识（"primary" / "alt"）
        "pub": fmt_addr(addr),             # 服务器观察到的公网 ip:port
    })


def handle_time_req(sock, msg, addr):
    """NTP 式时间同步：客户端发 t1（本地毫秒），服务器回 t1 回显 + t2（服务器毫秒）。

    客户端收到后用 offset = t2 - (t1 + t3) / 2 计算与服务器的时钟偏差，
    用于校准打洞时刻。
    """
    send_to(addr, {
        "type": T_TIME_REPLY,
        "ver": VER,
        "t1": msg.get("t1", 0),    # 回显客户端的发送时刻
        "t2": now_ms(),            # 服务器当前时刻
    })


def handle_udp_hello(sock, msg, addr):
    """登记客户端 UDP 打洞 socket 的公网映射（服务器从 recvfrom 的 addr 观察）。

    客户端加入房间后会用一个独立的本地 UDP 端口向服务器发 udp_hello，
    服务器记录的 addr 就是该 UDP socket 的 (公网IP, 公网端口)。之后对端
    打洞时直接用这个地址即可（前提：锥形 NAT，同一本地端口对外映射一致）。
    """
    mid = msg.get("id")
    if not mid:
        return
    with state_lock:
        udp_mappings[mid] = addr
        member = None
        info = id_index.get(mid)
        if info:
            member = rooms.get(info[0], {}).get(mid)
        if member:
            member["last_hb"] = now_ms()
    print("[UDP] 登记打洞映射 %s -> %s" % (mid, fmt_addr(addr)))


def dispatch(sock, data, addr):
    try:
        msg = json.loads(data.decode("utf-8"))
    except Exception:
        return
    t = msg.get("type")
    if t == T_JOIN:
        handle_join(sock, msg, addr)
    elif t == T_HB:
        handle_hb(sock, msg, addr)
    elif t == T_PUNCH_REQ:
        handle_punch_req(sock, msg, addr)
    elif t == T_BYE:
        handle_bye(sock, msg, addr)
    elif t == T_NAT_PROBE:
        handle_nat_probe(sock, msg, addr)
    elif t == T_TIME_REQ:
        handle_time_req(sock, msg, addr)
    elif t == T_UDP_HELLO:
        handle_udp_hello(sock, msg, addr)


# ==================== TCP 映射观测 ====================
def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _tcp_accept_loop(srv, tag):
    """某个地址族监听 socket 的 accept 循环。"""
    while True:
        try:
            conn, addr = srv.accept()
        except Exception as e:
            print("[TCP-%s] accept 异常: %s" % (tag, e))
            continue
        threading.Thread(target=_handle_tcp_reg, args=(conn, addr), daemon=True).start()


def tcp_listener_loop():
    """双栈监听 TCP（IPv4 + IPv6），记录客户端观测到的公网映射。

    IPv6 socket 设 IPV6_V6ONLY=1，避免与 IPv4 socket 端口冲突。
    """
    # IPv4
    try:
        s4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s4.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s4.bind((LISTEN_IP, TCP_LISTEN_PORT))
        s4.listen(128)
        threading.Thread(target=_tcp_accept_loop, args=(s4, "v4"), daemon=True).start()
    except Exception as e:
        print("[TCP] IPv4 监听失败: %s" % e)
    # IPv6（双栈内核上需 V6ONLY，避免与 v4 抢端口）
    try:
        s6 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        s6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s6.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        except Exception:
            pass
        s6.bind(("::", TCP_LISTEN_PORT))
        s6.listen(128)
        threading.Thread(target=_tcp_accept_loop, args=(s6, "v6"), daemon=True).start()
    except Exception as e:
        print("[TCP] IPv6 监听失败（可能系统不支持）: %s" % e)


def _handle_tcp_reg(conn, addr):
    """读取客户端上报的 id，登记其 TCP 公网映射，并挂起保持连接。"""
    conn.settimeout(10)
    try:
        raw = recv_exact(conn, 2)
        if not raw:
            conn.close(); return
        n = struct.unpack("!H", raw)[0]
        mid = (recv_exact(conn, n) or b"").decode("utf-8")
        if not mid:
            conn.close(); return
    except Exception:
        try: conn.close()
        except Exception: pass
        return

    with state_lock:
        tcp_mappings[mid] = addr
        old = tcp_socks.get(mid)
        tcp_socks[mid] = conn
        member = None
        info = id_index.get(mid)
        if info:
            member = rooms.get(info[0], {}).get(mid)
        if member:
            member["last_hb"] = now_ms()
    if old:
        try: old.close()
        except Exception: pass
    print("[TCP] 登记映射 %s -> %s" % (mid, fmt_addr(addr)))

    # 挂起保持连接（读阻塞直到对端关闭）
    try:
        conn.settimeout(None)
        while True:
            data = conn.recv(1024)
            if not data:
                break
    except Exception:
        pass
    finally:
        with state_lock:
            if tcp_socks.get(mid) is conn:
                tcp_socks.pop(mid, None)
                tcp_mappings.pop(mid, None)
        try: conn.close()
        except Exception: pass


# ==================== 后台清理 ====================
def cleanup_loop(sock):
    last_save = time.time()
    while True:
        time.sleep(CLEAN_INTERVAL)
        deadline = now_ms() - HEARTBEAT_TIMEOUT * 1000
        with state_lock:
            stale = [mid for mid, (room, _) in id_index.items()
                     if rooms.get(room, {}).get(mid, {}).get("last_hb", 0) < deadline]
        for mid in stale:
            remove_member(sock, mid)
        # 周期性保存状态（崩溃后最多丢 STATE_SAVE_INTERVAL 秒的变化）
        if time.time() - last_save >= STATE_SAVE_INTERVAL:
            save_state()
            last_save = time.time()


# ==================== 入口 ====================
def _print_startup_banner():
    """打印启动横幅（含局域网/公网地址）。

    在后台线程执行：公网 IP 探测可能耗时数秒，不能阻塞主线程的收包循环，
    否则启动瞬间客户端连不上服务器。
    """
    print("=" * 64)
    print("信令服务器已启动")
    print("  监听: UDP %s:%d（信令）| UDP %s:%d（NAT探测）| TCP %s:%d（映射观测）"
          % (LISTEN_IP, LISTEN_PORT, LISTEN_IP, NAT_PROBE_PORT, LISTEN_IP, TCP_LISTEN_PORT))
    print("  （0.0.0.0 表示监听本机所有网卡，下面才是实际可用地址）")
    lan_ips = get_lan_ips()
    if lan_ips:
        print("  局域网地址（同一网络内可直连）:")
        for ip in lan_ips:
            print("    %s   ->  客户端服务器填: %s" % (ip, ip))
    else:
        print("  局域网地址: 未获取到")
    print("  正在获取公网 IP ...")
    pub = get_public_ip()
    if pub:
        print("  公网地址（异地互传用）:")
        print("    %s   ->  客户端服务器填: %s" % (pub, pub))
    else:
        print("  公网地址: 未获取到（不影响运行；")
        print("            若异地互传，请手动确认服务器公网 IP 并填入客户端）")
    print("  提示: 客户端只需填 IP，端口默认 UDP %d / TCP %d" % (LISTEN_PORT, TCP_LISTEN_PORT))
    print("=" * 64)


def _udp_recv_loop(sock, tag):
    """某个 UDP socket 的收包循环。"""
    while True:
        try:
            data, addr = sock.recvfrom(MAX_PACKET)
        except Exception as e:
            print("[接收异常-%s] %s" % (tag, e))
            continue
        dispatch(sock, data, addr)


def main():
    global _udp4, _udp6

    load_config()
    load_state()  # 恢复上次的房间成员表（跨重启保持房间）

    # IPv4 UDP（信令）
    s4 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s4.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s4.bind((LISTEN_IP, LISTEN_PORT))
    _udp4 = s4

    # IPv6 UDP（信令，双栈支持）
    try:
        s6 = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        s6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s6.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        except Exception:
            pass
        s6.bind(("::", LISTEN_PORT))
        _udp6 = s6
    except Exception as e:
        print("[UDP] IPv6 监听失败（可能系统不支持）: %s" % e)

    # ---------- NAT 探测端口（独立的第 2 个 UDP 端口，用于对称 NAT 判定） ----------
    # 只接收 nat_probe 消息；回复走 _udp4/_udp6（源端口 3336 无所谓，
    # 客户端只关心自己发出的包被服务器从哪个源端口观察到）。
    probe4 = None
    probe6 = None
    try:
        probe4 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe4.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe4.bind((LISTEN_IP, NAT_PROBE_PORT))
        print("[NAT] 探测端口已监听 UDP %s:%d（IPv4）" % (LISTEN_IP, NAT_PROBE_PORT))
    except Exception as e:
        print("[NAT] IPv4 探测端口 %d 监听失败: %s" % (NAT_PROBE_PORT, e))
    try:
        probe6 = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        probe6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe6.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        except Exception:
            pass
        probe6.bind(("::", NAT_PROBE_PORT))
        print("[NAT] 探测端口已监听 UDP [::]:%d（IPv6）" % NAT_PROBE_PORT)
    except Exception as e:
        print("[NAT] IPv6 探测端口 %d 监听失败: %s" % (NAT_PROBE_PORT, e))

    # 先启动监听/收包线程，横幅在后台线程打印（公网 IP 探测耗时，不能阻塞收包）
    threading.Thread(target=tcp_listener_loop, daemon=True).start()
    threading.Thread(target=cleanup_loop, args=(s4,), daemon=True).start()
    threading.Thread(target=_print_startup_banner, daemon=True).start()

    if _udp6 is not None:
        threading.Thread(target=_udp_recv_loop, args=(s6, "v6"), daemon=True).start()
    if probe4 is not None:
        threading.Thread(target=_udp_recv_loop, args=(probe4, "nat4"), daemon=True).start()
    if probe6 is not None:
        threading.Thread(target=_udp_recv_loop, args=(probe6, "nat6"), daemon=True).start()

    # 主线程跑 IPv4 收包循环
    _udp_recv_loop(s4, "v4")


if __name__ == "__main__":
    import atexit
    atexit.register(save_state)  # 退出时保存状态
    try:
        main()
    except KeyboardInterrupt:
        print("\n[退出] 收到 Ctrl+C")
    finally:
        save_state()

