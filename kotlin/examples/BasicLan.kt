package examples

import io.dpmp.link.Discovery
import io.dpmp.link.LinkManager
import io.dpmp.util.Net
import java.io.File

/**
 * DPMP 最小示例（Kotlin）：局域网设备发现。
 * 与 Python 端 examples/basic_lan.py 对应。
 */
fun main() {
    val did = Net.getOrCreateDeviceId(System.getProperty("user.home") + File.separator + ".dpmp_config.json")

    val disc = Discovery(
        deviceId = did,
        hostname = "demo",
        log = { println(it) },
        onNewNode = { ip, msg -> println("[新设备] " + ip + " " + msg) }
    )
    disc.start()
    disc.broadcastSearch()

    val lm = LinkManager(null, localTcpPort = 9998, log = { println(it) })
    lm.onSocketReady = { peerId, _, _ ->
        println("[连接就绪] " + peerId + "，可以在这里跑你自己的协议")
    }
    lm.start()

    println("发现的设备：")
    for ((ip, n) in disc.getNodes()) {
        println("  " + ip + "  " + n["hostname"])
    }

    disc.stop()
    lm.stop()
}
