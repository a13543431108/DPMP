# -*- coding: utf-8 -*-
"""DPMP 最小示例：局域网设备发现。

本示例只演示「发现」——通过 UDP 广播/扫描找到同网段的 DPMP 设备。
要「真正建立连接并互发消息」，见同目录 echo.py（房间模式端到端示例）。
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dpmp.link.discovery import Discovery
from dpmp.link.manager import LinkManager
from dpmp.util.net import get_or_create_device_id


def main():
    did = get_or_create_device_id("~/.dpmp_config.json")

    disc = Discovery(did, hostname="demo", log=print,
                     on_new_node=lambda ip, msg: print("[新设备] %s %s" % (ip, msg)))
    disc.start()
    disc.broadcast_search()

    # 局域网模式无信令服务器，LinkManager 的 signaling 传 None
    lm = LinkManager(None, local_tcp_port=9998, log=print)

    def on_ready(peer_id, sock, member):
        print("[连接就绪] %s，可以在这里跑你自己的协议" % peer_id)
        # 例：sock.sendall(b"hello")

    lm.on_socket_ready = on_ready
    lm.start()

    print("发现的设备：")
    for ip, n in disc.get_nodes().items():
        print("  %s  %s" % (ip, n.get("hostname")))

    disc.stop()
    lm.stop()


if __name__ == "__main__":
    main()
