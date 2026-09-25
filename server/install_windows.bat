@echo off
chcp 65001 >nul
REM 信令服务器 —— Windows 一键部署脚本
REM 用法：右键"以管理员身份运行"

setlocal
set APP_NAME=P2PSignalServer
set SRC=%~dp0
set INSTALL_DIR=%ProgramData%\%APP_NAME%

echo ==== 安装信令服务器 ====

REM 1. 检查 Python
python --version >nul 2>&1
if errorlevel 1 (
  echo [错误] 未找到 python，请先安装 Python 3.7+ 并加入 PATH
  pause
  exit /b 1
)
for /f "delims=" %%v in ('python --version 2^>^&1') do echo [1/4] %%v

REM 2. 复制文件
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"
copy /Y "%SRC%信令服务器.py" "%INSTALL_DIR%\" >nul
if exist "%SRC%config.json" copy /Y "%SRC%config.json" "%INSTALL_DIR%\" >nul
echo [2/4] 已安装到 %INSTALL_DIR%

REM 3. 放行防火墙端口
set PORT=3336
set TCPPORT=3337
netsh advfirewall firewall delete rule name="%APP_NAME% UDP" >nul 2>&1
netsh advfirewall firewall delete rule name="%APP_NAME% TCP" >nul 2>&1
netsh advfirewall firewall add rule name="%APP_NAME% UDP" dir=in action=allow protocol=UDP localport=%PORT% >nul
netsh advfirewall firewall add rule name="%APP_NAME% TCP" dir=in action=allow protocol=TCP localport=%TCPPORT% >nul
echo [3/4] 防火墙已放行 UDP %PORT% / TCP %TCPPORT%

REM 4. 注册计划任务（开机自启 + 崩溃重启）
schtasks /delete /tn "%APP_NAME%" /f >nul 2>&1
schtasks /create /tn "%APP_NAME%" /tr "python \"%INSTALL_DIR%\信令服务器.py\"" /sc onstart /ru SYSTEM /rl HIGHEST /f >nul
schtasks /run /tn "%APP_NAME%" >nul
echo [4/4] 已注册开机自启任务

echo.
echo ==== 安装完成 ====
echo 启动:   schtasks /run    /tn "%APP_NAME%"
echo 停止:   schtasks /end    /tn "%APP_NAME%"
echo 状态:   schtasks /query  /tn "%APP_NAME%"
echo 日志:   %INSTALL_DIR%\server.log
echo 配置:   %INSTALL_DIR%\config.json
echo.
pause
