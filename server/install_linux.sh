#!/usr/bin/env bash
# 信令服务器 —— Linux 一键部署脚本
# 用法：sudo bash install_linux.sh
set -e

APP_NAME="p2p-signal"
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="/opt/$APP_NAME"
SERVICE_FILE="/etc/systemd/system/$APP_NAME.service"

echo "==== 安装信令服务器 ===="

# 1. 检查 Python
if ! command -v python3 >/dev/null 2>&1; then
  echo "[错误] 未找到 python3，请先安装：apt install python3  或  yum install python3"
  exit 1
fi
echo "[1/4] Python: $(python3 --version)"

# 2. 复制到安装目录
mkdir -p "$INSTALL_DIR"
cp "$SRC_DIR/信令服务器.py" "$INSTALL_DIR/"
[ -f "$SRC_DIR/config.json" ] && cp "$SRC_DIR/config.json" "$INSTALL_DIR/"
echo "[2/4] 已安装到 $INSTALL_DIR"

# 3. 写 systemd 服务
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=P2P Signal Server (房间匹配 + 打洞协调)
After=network.target

[Service]
Type=simple
WorkingDirectory=$INSTALL_DIR
ExecStart=/usr/bin/env python3 $INSTALL_DIR/信令服务器.py
Restart=always
RestartSec=3
StandardOutput=append:$INSTALL_DIR/server.log
StandardError=append:$INSTALL_DIR/server.log

[Install]
WantedBy=multi-user.target
EOF
echo "[3/4] 已写入服务: $SERVICE_FILE"

# 4. 放行端口（若安装了防火墙）
PORT=$(python3 -c "import json;print(json.load(open('$INSTALL_DIR/config.json')).get('LISTEN_PORT',3336))" 2>/dev/null || echo 3336)
TCPPORT=$((PORT + 1))
if command -v ufw >/dev/null 2>&1; then
  ufw allow ${PORT}/udp >/dev/null 2>&1 || true
  ufw allow ${TCPPORT}/tcp >/dev/null 2>&1 || true
  echo "[4/4] ufw 已放行 UDP $PORT / TCP $TCPPORT"
elif command -v firewall-cmd >/dev/null 2>&1; then
  firewall-cmd --permanent --add-port=${PORT}/udp >/dev/null 2>&1 || true
  firewall-cmd --permanent --add-port=${TCPPORT}/tcp >/dev/null 2>&1 || true
  firewall-cmd --reload >/dev/null 2>&1 || true
  echo "[4/4] firewalld 已放行 UDP $PORT / TCP $TCPPORT"
else
  echo "[4/4] 未检测到 ufw/firewalld，请手动放行 UDP $PORT / TCP $TCPPORT"
fi

systemctl daemon-reload
systemctl enable "$APP_NAME"
systemctl restart "$APP_NAME"
sleep 1
systemctl --no-pager status "$APP_NAME" || true

echo ""
echo "==== 安装完成 ===="
echo "启动:   systemctl start  $APP_NAME"
echo "停止:   systemctl stop   $APP_NAME"
echo "重启:   systemctl restart $APP_NAME"
echo "状态:   systemctl status $APP_NAME"
echo "日志:   tail -f $INSTALL_DIR/server.log"
echo "配置:   $INSTALL_DIR/config.json"
