package examples

import io.dpmp.link.LinkManager
import io.dpmp.link.SignalingClient
import io.dpmp.util.Net
import java.io.File

/**
 * DPMP 端到端示例（Kotlin）：两台设备通过「房间号」互发消息。
 *
 * 与 Python 端 examples/echo.py 对应。
 *
 * 用法：
 *     设备 A：kotlin ...EchoKt --server 1.2.3.4 --room myroom --name Alice
 *     设备 B：kotlin ...EchoKt --server 1.2.3.4 --room myroom --name Bob
 *
 * 运行时：
 *     · 直接输入文字并回车 → 广播给房间内所有已连接的对端
 *     · 收到对端消息 → 自动打印，并回一句 "echo: <原消息>"
 *     · 输入 /quit 退出
 */
fun main(args: Array<String>) {
    var server: String? = null
    var port = 3336
    var room: String? = null
    var name: String? = null
    var tcpPort = 9998
    var udpHolePort = 9996

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--server" -> { server = args.getOrNull(i + 1); i++ }
            "--port" -> { port = args.getOrNull(i + 1)?.toIntOrNull() ?: 3336; i++ }
            "--room" -> { room = args.getOrNull(i + 1); i++ }
            "--name" -> { name = args.getOrNull(i + 1); i++ }
            "--tcp-port" -> { tcpPort = args.getOrNull(i + 1)?.toIntOrNull() ?: 9998; i++ }
            "--udp-hole-port" -> { udpHolePort = args.getOrNull(i + 1)?.toIntOrNull() ?: 9996; i++ }
        }
        i++
    }
    if (room == null || name == null) {
        println("用法: --room <房间号> --name <显示名> [--server ip] [--port 3336]")
        return
    }

    val did = Net.getOrCreateDeviceId(System.getProperty("user.home") + File.separator + ".dpmp_config.json")

    val sig = SignalingClient(
        serverIp = server,
        serverPort = if (server != null) port else null,
        room = room,
        name = name,
        tcpPort = tcpPort,
        punchLocalPort = tcpPort,
        udpHolePort = udpHolePort,
        deviceId = did,
        log = { println("[信令] " + it) }
    )

    val lm = LinkManager(sig, localTcpPort = tcpPort, log = { println("[连接] " + it) })

    sig.onJoined = { lm.onJoined(it) }
    sig.onMemberJoin = { lm.onMemberJoin(it) }
    sig.onMemberLeave = { lm.onMemberLeave(it) }
    sig.onPunchGo = { peer, at -> lm.onPunchGo(peer, at) }
    sig.onUdpHoleReady = { pid, addr -> lm.onUdpHoleReady(pid, addr) }
    sig.onMappingReady = { lm.markMappingReady() }

    lm.onSocketReady = { peerId, _, member ->
        val who = member["name"] as? String ?: peerId
        println("[+] 已连接: " + who + " (" + peerId + ")")
        Thread({ recvLoop(lm, peerId, who) }, "echo-recv").apply { isDaemon = true; start() }
    }

    lm.start()
    if (!sig.start()) {
        println("[错误] 无法连接信令服务器")
        return
    }
    println("[*] 已加入房间 " + room + "，等待对端...（输入文字回车发送，/quit 退出）")

    while (true) {
        val line = readLine() ?: break
        val text = line.trim()
        if (text.isEmpty()) continue
        if (text == "/quit") break
        val members = lm.getMembers()
        if (members.isEmpty()) { println("[!] 暂无已连接对端"); continue }
        for (m in members) {
            if (m.state != "connected") continue
            val ch = lm.getSendChannel(m.id) ?: continue
            try {
                synchronized(ch.ioLock) { ch.sock.sendall(text.toByteArray(Charsets.UTF_8)) }
                println("[>] -> " + m.name + ": " + text)
            } catch (e: Exception) {
                println("[!] 发送失败: " + e.message)
            }
        }
    }
    sig.stop()
    lm.stop()
    println("[*] 已退出")
}

private fun recvLoop(lm: LinkManager, peerId: String, who: String) {
    while (lm.hasPeer(peerId)) {
        val ch = lm.getSendChannel(peerId)
        if (ch == null) { Thread.sleep(500); continue }
        val data = try { ch.sock.recv(4096) } catch (e: Exception) { Thread.sleep(200); continue }
        if (data.isEmpty()) { Thread.sleep(200); continue }
        val text = String(data, Charsets.UTF_8).trim()
        println("[<] " + who + ": " + text)
        try {
            synchronized(ch.ioLock) { ch.sock.sendall(("echo: " + text).toByteArray(Charsets.UTF_8)) }
        } catch (e: Exception) {
            println("[!] 回显失败: " + e.message)
        }
    }
}
