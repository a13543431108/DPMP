# 信令服务器

公网房间匹配与打洞协调服务器。**不转发任何文件数据**，只处理几十字节的 UDP 小包。

## 职责

1. 设备按“房间号”注册上线，服务器维护房间成员表
2. 向新成员下发房间现有成员地址，向老成员广播新成员加入
3. 协调两成员同时打洞（下发统一打洞时刻 `at`）
4. 心跳保活，超时成员剔除并广播离开
5. **房间密码**（协议版本 2）：首位加入者决定房间性质——
   不带密码 = 开放房间（任何人凭房间号可进）；带密码 = 受保护房间
   （须密码匹配）。服务器只存 `sha256("room:pwd")`，不回传、不落明文。
   房间空了密码一并删除。详见 `DPMP_PROTOCOL.md` §3.1。

---

## 目录文件

| 文件 | 用途 |
|---|---|
| `信令服务器.py` | 主程序（Python 3.7+，纯标准库） |
| `启动.py` | 跨平台启动器（检查环境、透传参数） |
| `config.example.json` | 配置模板（复制为 config.json 生效） |
| `install_linux.sh` | Linux 一键部署（systemd 服务） |
| `install_windows.bat` | Windows 一键部署（计划任务） |

---

## 快速开始

### 方式一：直接运行（调试）

```bash
python 信令服务器.py
# 或
python 启动.py
```

### 方式二：一键部署（生产）

**Linux**（需 root）：
```bash
sudo bash install_linux.sh
```
- 安装到 `/opt/p2p-signal`
- 注册 systemd 服务（开机自启 + 崩溃自动重启）
- 自动放行 ufw/firewalld 端口

**Windows**（以管理员身份运行 `install_windows.bat`）：
- 安装到 `%ProgramData%\P2PSignalServer`
- 注册计划任务（开机自启）
- 自动放行 Windows 防火墙

---

## 配置（三级覆盖，无需改源码）

配置优先级（从低到高）：

1. **源码内默认值**
2. **config.json**（同目录，或 `--config` 指定）
3. **命令行参数**

### 方式一：config.json
复制 `config.example.json` 为 `config.json`，修改即可：

```json
{
  "LISTEN_PORT": 3336,
  "MAX_ROOM_SIZE": 16
}
```

### 方式二：命令行
```bash
python 信令服务器.py --port 4000 --max-room 32
python 信令服务器.py --config /path/to/my.json
python 信令服务器.py --ip 0.0.0.0 --heartbeat-timeout 60
```

### 全部可配置项

| 变量 | 默认 | 说明 |
|---|---|---|
| `LISTEN_IP` | 0.0.0.0 | 监听地址 |
| `LISTEN_PORT` | 3336 | UDP 信令端口 |
| `TCP_LISTEN_PORT` | 3337 | TCP 映射观测端口 |
| `MAX_ROOM_SIZE` | 16 | 单房间最大人数 |
| `HEARTBEAT_TIMEOUT` | 30 | 成员心跳超时（秒） |
| `CLEAN_INTERVAL` | 5 | 清理线程扫描间隔（秒） |
| `PUNCH_DELAY_MS` | 300 | 下发 punch_go 后延迟（毫秒） |

---

## 服务管理

### Linux（systemd）
```bash
systemctl start  p2p-signal    # 启动
systemctl stop   p2p-signal    # 停止
systemctl restart p2p-signal   # 重启
systemctl status p2p-signal    # 状态
tail -f /opt/p2p-signal/server.log   # 日志
```

### Windows（计划任务）
```cmd
schtasks /run   /tn P2PSignalServer    # 启动
schtasks /end   /tn P2PSignalServer    # 停止
schtasks /query /tn P2PSignalServer    # 状态
```

---

## 部署注意

- 服务器需有**公网 IP**，防火墙放行 **UDP 3336 + TCP 3337**（脚本已自动处理）
- 服务器带宽要求极低：每次交互仅几十字节
- 客户端需知道“服务器公网 IP（+ 端口，若改了默认端口）”
- 启动时会打印局域网 IP 与公网 IP，供客户端填写

---

## 协议（UDP + JSON）

### 客户端 → 服务器

| type | 字段 | 说明 |
|---|---|---|
| `join` | room, name, tcp, lan[], did | 加入房间 |
| `hb` | id | 心跳（每 20s） |
| `punch_req` | id, target | 请求与某成员打洞 |
| `bye` | id | 离开房间 |

### 服务器 → 客户端

| type | 字段 | 说明 |
|---|---|---|
| `joined` | id, members[] | 加入成功，返回自己 ID + 房间现有成员 |
| `member_join` | member | 有新成员加入 |
| `member_leave` | id | 成员离线 |
| `punch_go` | peer, at | 打洞协调：对端地址 + 统一打洞时刻(ms) |
| `error` | code | 错误（如 ROOM_FULL） |

成员的 `pub`（公网地址）由服务器从 `recvfrom` 的 `addr` 自动填充；`pub_tcp`（TCP 公网映射）从 TCP 观测端口获得。IPv6 地址用 `[ip]:port` 格式。
