#!/bin/bash

echo "🚀 [Socat Wrapper] Starting Socat port forwarders..."

# 转发实盘端口: 监听 4003 (任意IP) -> 转发给本机 4001 (API)
socat TCP-LISTEN:4003,fork,bind=0.0.0.0 TCP:127.0.0.1:4001 &
echo "✅ Forwarding 0.0.0.0:4003 -> 127.0.0.1:4001 (Live)"

# 转发模拟盘端口: 监听 4004 (任意IP) -> 转发给本机 4002 (API)
socat TCP-LISTEN:4004,fork,bind=0.0.0.0 TCP:127.0.0.1:4002 &
echo "✅ Forwarding 0.0.0.0:4004 -> 127.0.0.1:4002 (Paper)"

echo "🚀 [Socat Wrapper] Starting IB Gateway..."
# 执行原镜像的启动命令
exec /home/ibgateway/scripts/run.sh