FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制脚本
COPY mongodb_to_bytehouse.py .

# 创建入口脚本
RUN echo '#!/bin/bash\npython3 mongodb_to_bytehouse.py "$@"' > entrypoint.sh
RUN chmod +x entrypoint.sh

# 创建日志目录
RUN mkdir -p /app/logs

# 环境变量
ENV MONGO_URI= \
    MONGO_DATABASE= \
    MONGO_AUTH_SOURCE=admin \
    TARGET_BH_HOST= \
    TARGET_BH_PORT=19000 \
    TARGET_BH_USER=bytehouse \
    TARGET_BH_PASSWORD= \
    TARGET_BH_DATABASE= \
    SYNC_BATCH_SIZE=1000 \
    INCREMENTAL_INTERVAL=60 \
    LOG_LEVEL=INFO \
    LOG_FILE= \
    FEISHU_WEBHOOK=

ENTRYPOINT ["/app/entrypoint.sh"]
