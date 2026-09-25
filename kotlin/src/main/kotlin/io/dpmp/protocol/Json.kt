package io.dpmp.protocol

/**
 * 轻量 JSON 编解码（纯标准库，零第三方）。
 *
 * 支持类型：Map<String, Any?> / List<Any?> / String / Number / Boolean / null。
 * 用于信令（JSON over UDP）与局域网发现消息。
 *
 * 编解码保持与 Python 端 json.dumps / json.loads 的互操作性：
 *  · 数字统一按 Long/Double 输出（整数不带小数点）
 *  · 字符串按 UTF-8，非 ASCII 直接输出（不转 \\uXXXX）
 */
object Json {

    // ============================================================
    // 编码
    // ============================================================

    fun encode(value: Any?): String {
        val sb = StringBuilder()
        writeValue(sb, value)
        return sb.toString()
    }

    private fun writeValue(sb: StringBuilder, value: Any?) {
        when (value) {
            null -> sb.append("null")
            is String -> writeString(sb, value)
            is Boolean -> sb.append(if (value) "true" else "false")
            is Int -> sb.append(value.toString())
            is Long -> sb.append(value.toString())
            is Short -> sb.append(value.toString())
            is Byte -> sb.append(value.toString())
            is Float -> writeNumber(sb, value.toDouble())
            is Double -> writeNumber(sb, value)
            is Map<*, *> -> writeObject(sb, value)
            is List<*> -> writeArray(sb, value)
            is Array<*> -> writeArray(sb, value.toList())
            is IntArray -> writeArray(sb, value.toList())
            else -> writeString(sb, value.toString())
        }
    }

    private fun writeNumber(sb: StringBuilder, d: Double) {
        if (d.isNaN() || d.isInfinite()) {
            sb.append("null")
        } else if (d == Math.floor(d) && !d.isInfinite() && Math.abs(d) < 1e15) {
            sb.append(d.toLong().toString())
        } else {
            sb.append(d.toString())
        }
    }

    private fun writeObject(sb: StringBuilder, map: Map<*, *>) {
        sb.append('{')
        var first = true
        for ((k, v) in map) {
            if (!first) sb.append(',')
            first = false
            writeString(sb, k.toString())
            sb.append(':')
            writeValue(sb, v)
        }
        sb.append('}')
    }

    private fun writeArray(sb: StringBuilder, list: List<*>) {
        sb.append('[')
        var first = true
        for (v in list) {
            if (!first) sb.append(',')
            first = false
            writeValue(sb, v)
        }
        sb.append(']')
    }

    private fun writeString(sb: StringBuilder, s: String) {
        sb.append('"')
        for (c in s) {
            when (c) {
                '"' -> sb.append("\\\"")
                '\\' -> sb.append("\\\\")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                '\b' -> sb.append("\\b")
                '\u000C' -> sb.append("\\f")
                else -> if (c < ' ') {
                    sb.append(String.format("\\u%04x", c.code))
                } else {
                    sb.append(c)
                }
            }
        }
        sb.append('"')
    }

    // ============================================================
    // 解码
    // ============================================================

    fun decode(text: String): Map<String, Any?> {
        val parser = Parser(text)
        parser.skipWs()
        val v = parser.parseValue()
        return (v as? Map<*, *>)?.entries?.associate { it.key.toString() to it.value } ?: emptyMap()
    }

    private class Parser(private val s: String) {
        private var pos = 0

        fun skipWs() {
            while (pos < s.length && s[pos].isWhitespace()) pos++
        }

        fun parseValue(): Any? {
            skipWs()
            if (pos >= s.length) return null
            return when (s[pos]) {
                '{' -> parseObject()
                '[' -> parseArray()
                '"' -> parseString()
                't' -> { expect("true"); true }
                'f' -> { expect("false"); false }
                'n' -> { expect("null"); null }
                else -> parseNumber()
            }
        }

        private fun expect(word: String) {
            if (pos + word.length <= s.length && s.substring(pos, pos + word.length) == word) {
                pos += word.length
            }
        }

        private fun parseObject(): Map<String, Any?> {
            val out = LinkedHashMap<String, Any?>()
            pos++ // {
            skipWs()
            if (pos < s.length && s[pos] == '}') { pos++; return out }
            while (pos < s.length) {
                skipWs()
                val key = parseString()
                skipWs()
                if (pos < s.length && s[pos] == ':') pos++
                val value = parseValue()
                out[key] = value
                skipWs()
                if (pos < s.length && s[pos] == ',') { pos++; continue }
                if (pos < s.length && s[pos] == '}') { pos++; break }
                break
            }
            return out
        }

        private fun parseArray(): List<Any?> {
            val out = ArrayList<Any?>()
            pos++ // [
            skipWs()
            if (pos < s.length && s[pos] == ']') { pos++; return out }
            while (pos < s.length) {
                val value = parseValue()
                out.add(value)
                skipWs()
                if (pos < s.length && s[pos] == ',') { pos++; continue }
                if (pos < s.length && s[pos] == ']') { pos++; break }
                break
            }
            return out
        }

        private fun parseString(): String {
            skipWs()
            if (pos >= s.length || s[pos] != '"') return ""
            pos++ // opening quote
            val sb = StringBuilder()
            while (pos < s.length) {
                val c = s[pos]
                if (c == '"') { pos++; break }
                if (c == '\\') {
                    pos++
                    if (pos >= s.length) break
                    when (s[pos]) {
                        '"' -> sb.append('"')
                        '\\' -> sb.append('\\')
                        '/' -> sb.append('/')
                        'n' -> sb.append('\n')
                        'r' -> sb.append('\r')
                        't' -> sb.append('\t')
                        'b' -> sb.append('\b')
                        'f' -> sb.append('\u000C')
                        'u' -> {
                            if (pos + 4 < s.length) {
                                val hex = s.substring(pos + 1, pos + 5)
                                val code = hex.toIntOrNull(16)
                                if (code != null) sb.append(code.toChar())
                                pos += 4
                            }
                        }
                        else -> sb.append(s[pos])
                    }
                    pos++
                } else {
                    sb.append(c)
                    pos++
                }
            }
            return sb.toString()
        }

        private fun parseNumber(): Any? {
            skipWs()
            val start = pos
            if (pos < s.length && (s[pos] == '-' || s[pos] == '+')) pos++
            var isFloat = false
            while (pos < s.length) {
                val c = s[pos]
                if (c.isDigit()) { pos++ }
                else if (c == '.' || c == 'e' || c == 'E' || c == '-' || c == '+') { isFloat = true; pos++ }
                else break
            }
            val token = s.substring(start, pos)
            if (token.isEmpty()) return null
            return if (isFloat) token.toDoubleOrNull() else token.toLongOrNull()
        }
    }
}
