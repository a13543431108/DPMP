package io.dpmp.link

import io.dpmp.Config
import io.dpmp.protocol.C
import java.util.concurrent.locks.ReentrantLock

/**
 * 单条路径的保活间隔调度（软性探测 + 硬性下限）。
 * 与 Python 端 dpmp/link/path.py 的 KeepaliveScheduler 对齐。
 */
class KeepaliveScheduler(
    var role: String,
    val proto: String,
    private val config: Config = Config.DEFAULT
) {
    private val floor: Int = config.protoFloor[proto] ?: 15
    private val safeCap: Int = config.protoSafeCap[proto] ?: 60

    var currentInterval: Int = floor * 2
    var upperBound: Int? = null
    var lowerBound: Int? = null
    var successCount: Int = 0
    var failCount: Int = 0
    var stable: Boolean = false

    private fun clamp(v0: Int): Int {
        var v = maxOf(v0, floor)
        v = if (role == C.ROLE_WARM_SAFE) minOf(v, safeCap) else minOf(v, 3600)
        return v
    }

    fun onSuccess() {
        successCount += 1
        upperBound = currentInterval
        if (stable) return
        val total = successCount + failCount
        if (total >= 5 && failCount.toDouble() / (total + 1) > 0.05) {
            stable = true
            return
        }
        var c = (currentInterval * 2 * (config.roleFactor[role] ?: 1.0)).toInt()
        if (lowerBound != null) {
            c = minOf(c, ((upperBound ?: 0) + (lowerBound ?: 0)) / 2)
            stable = true
        }
        currentInterval = clamp(c)
    }

    fun onFailure() {
        failCount += 1
        lowerBound = currentInterval
        stable = false
        currentInterval = if (upperBound != null) {
            ((upperBound ?: 0) + (lowerBound ?: 0)) / 2
        } else {
            floor
        }
        currentInterval = clamp(currentInterval)
        if (failCount >= 3) currentInterval = floor
    }
}

/** 单条路径的运行时状态。 */
class Path(
    val pathId: String,
    val proto: String,
    role: String,
    var rttMs: Int = 0,
    private val config: Config = Config.DEFAULT
) {
    var role: String = role
        private set
    var scheduler: KeepaliveScheduler = KeepaliveScheduler(role, proto, config)
        private set
    @Volatile var lastSeen: Double = nowSec()

    fun setRole(role: String) {
        this.role = role
        this.scheduler = KeepaliveScheduler(role, proto, config)
    }

    /** 内部使用：直接改 role，不重建 scheduler（用于 promote 逻辑）。 */
    internal fun setRoleRaw(role: String) { this.role = role }

    companion object {
        private fun nowSec(): Double = System.nanoTime() / 1_000_000_000.0
    }
}

/**
 * 单个 peer 的路径分级与保活决策。
 *
 * 路径来源：TCP 打洞成功 → "tcp" 路径；UDP 打洞成功 → "udp" 路径。
 * 按协议优先级（TCP > UDP）+ RTT 分级。
 *
 * 与 Python 端 dpmp/link/path.py 的 PeerPathScheduler 对齐。
 */
class PeerPathScheduler(
    val peerId: String,
    private val log: (String) -> Unit = {},
    private val config: Config = Config.DEFAULT
) {
    val lock = ReentrantLock()
    val paths: MutableMap<String, Path> = LinkedHashMap()

    /** 新路径接入。已存在且未失效则直接复用，否则新建并重排角色。 */
    fun register(pathId: String, proto: String, rttMs: Int): Path {
        lock.lock()
        var p: Path
        try {
            val existing = paths[pathId]
            if (existing != null && existing.role != C.ROLE_DEAD) return existing
            p = Path(pathId, proto, C.ROLE_WARM_LOOSE, rttMs, config)
            paths[pathId] = p
            regradeLocked()
        } finally {
            lock.unlock()
        }
        log("[路径] peer=" + peerId + " 注册 " + pathId + "/" + proto + " rtt=" + rttMs + "ms role=" + p.role)
        return p
    }

    private fun regradeLocked() {
        val alive = paths.values.filter { it.role != C.ROLE_DEAD }
        val sorted = alive.sortedWith(compareBy({ config.protoPriority[it.proto] ?: 9 }, { it.rttMs }))
        val roles = listOf(C.ROLE_HOT, C.ROLE_WARM_SAFE, C.ROLE_WARM_LOOSE)
        for ((i, p) in sorted.withIndex()) {
            val newRole = if (i < roles.size) roles[i] else C.ROLE_WARM_LOOSE
            if (p.role != newRole) p.setRole(newRole)
        }
    }

    fun regrade() {
        lock.lock(); try { regradeLocked() } finally { lock.unlock() }
    }

    fun remove(pathId: String) {
        lock.lock(); try { paths.remove(pathId) } finally { lock.unlock() }
    }

    fun getByRole(role: String): Path? {
        lock.lock(); try { return paths.values.firstOrNull { it.role == role } } finally { lock.unlock() }
    }

    fun getHot(): Path? = getByRole(C.ROLE_HOT)

    /** 热备失效 → 保守暖备上位，宽松暖备升保守。返回新热备 pathId。 */
    fun promoteOnHotFailure(): String? {
        lock.lock()
        try {
            var hot: Path? = null; var safe: Path? = null; var loose: Path? = null
            for (p in paths.values) {
                when (p.role) {
                    C.ROLE_HOT -> hot = p
                    C.ROLE_WARM_SAFE -> safe = p
                    C.ROLE_WARM_LOOSE -> loose = p
                }
            }
            hot?.setRoleRaw(C.ROLE_DEAD)
            var newHot: String? = null
            if (safe != null) { safe.setRole(C.ROLE_HOT); newHot = safe.pathId }
            if (loose != null) { loose.setRole(C.ROLE_WARM_SAFE) }
            return newHot
        } finally {
            lock.unlock()
        }
    }

    fun roleOf(pathId: String): String? {
        lock.lock(); try { return paths[pathId]?.role } finally { lock.unlock() }
    }
}
