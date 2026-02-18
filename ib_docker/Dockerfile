# 基于你正在使用的稳定版镜像
FROM ghcr.io/gnzsnz/ib-gateway:stable

USER root

# 1. 安装 socat (为了精简，安装完清理缓存)
# 如果 apt-get 报错，说明基础镜像是 Alpine，请改用 apk add socat
RUN apt-get update && \
    apt-get install -y socat && \
    rm -rf /var/lib/apt/lists/*

# 2. 复制启动脚本进容器
COPY start-socat.sh /root/start-socat.sh

# 3. 赋予脚本执行权限
RUN chmod +x /root/start-socat.sh

# 4. 覆盖原镜像的 CMD，使用我们的 wrapper
CMD ["/root/start-socat.sh"]