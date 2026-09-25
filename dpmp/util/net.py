# -*- coding: utf-8 -*-
"""网络工具：本机 IP / 子网 / 广播地址 / 设备标识。"""

import ipaddress
import json
import os
import socket
import uuid


def get_all_local_ips(ipv6=False):
    """获取本机所有 IP 地址。ipv6=True 则包含 IPv6。"""
    ips = set()
    try:
        addrinfos = socket.getaddrinfo(socket.gethostname(), None,
                                       socket.AF_UNSPEC, socket.SOCK_STREAM)
        for info in addrinfos:
            addr = info[4][0]
            if ipv6:
                if ":" in addr and not addr.startswith("::1"):
                    ips.add(addr)
            else:
                if "." in addr and not addr.startswith("127."):
                    ips.add(addr)
    except Exception:
        pass
    if not ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 1))
            ips.add(s.getsockname()[0])
            s.close()
        except Exception:
            pass
    return list(ips)


def get_subnet_for_ip(ip_str):
    """推断某 IP 所属子网 CIDR。IPv6 暂不支持，返回 None。"""
    if ":" in ip_str:
        return None
    try:
        ipaddress.IPv4Address(ip_str)
        if ip_str.startswith("169.254"):
            return "%s/16" % ip_str
        elif ip_str.startswith("10."):
            return "%s/24" % ip_str
        elif ip_str.startswith("172.") and 16 <= int(ip_str.split(".")[1]) <= 31:
            return "%s/16" % ip_str
        else:
            return "%s/24" % ip_str
    except Exception:
        return None


def get_all_subnets():
    subnets = set()
    for ip in get_all_local_ips(ipv6=False):
        cidr = get_subnet_for_ip(ip)
        if cidr:
            subnets.add(cidr)
    return list(subnets)


def get_broadcast_addrs():
    addrs = set()
    for ip_str in get_all_local_ips(ipv6=False):
        cidr = get_subnet_for_ip(ip_str)
        if cidr:
            try:
                net = ipaddress.IPv4Network(cidr, strict=False)
                addrs.add(str(net.broadcast_address))
            except Exception:
                pass
    if not addrs:
        addrs.add("255.255.255.255")
    return list(addrs)


def get_mac_address():
    """尽力获取本机 MAC 地址（展示用，不作识别主键）。"""
    try:
        node = uuid.getnode()
        if (node >> 40) & 1:
            return ""
        hexs = "%012X" % node
        return ":".join(hexs[i:i + 2] for i in range(0, 12, 2))
    except Exception:
        return ""


def get_or_create_device_id(config_path):
    """获取或生成持久化的设备 UUID（稳定标识，跨重启不变）。

    config_path：配置文件路径（JSON），不存在则创建。
    """
    cfg = {}
    p = os.path.expanduser(config_path)
    if os.path.exists(p):
        try:
            with open(p, "r", encoding="utf-8") as f:
                cfg = json.load(f) or {}
        except Exception:
            cfg = {}
    did = cfg.get("device_id")
    if did:
        return did
    did = str(uuid.uuid4())
    cfg["device_id"] = did
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(cfg, f)
    except Exception:
        pass
    return did
