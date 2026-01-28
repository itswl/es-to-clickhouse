FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/python:3.11-slim

LABEL maintainer="ES to ByteHouse Sync Service"
LABEL description="Elasticsearch to ByteHouse data synchronization service"

# 设置工作目录
WORKDIR /app

# 设置时区
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用文件
COPY es_to_bytehouse.py .
COPY entrypoint.sh .

# 设置执行权限
RUN chmod +x /app/entrypoint.sh

# 默认环境变量
ENV ES_HOST=http://localhost:9200 \
    ES_USER=admin \
    ES_PASSWORD= \
    BYTEHOUSE_HOST=localhost \
    BYTEHOUSE_PORT=19000 \
    BYTEHOUSE_USER=bytehouse \
    BYTEHOUSE_PASSWORD= \
    BYTEHOUSE_SECURE=true \
    TARGET_DATABASE=es_migration \
    INDEX_PATTERN=* \
    BATCH_SIZE=1000 \
    SCROLL_SIZE=1000 \
    SCROLL_TIMEOUT=5m \
    INCREMENTAL_INTERVAL=60 \
    SKIP_FULL_SYNC=false \
    STORE_SOURCE=false \
    LOG_LEVEL=INFO \
    LOG_FILE=/app/logs/es_to_bytehouse.log \
    FEISHU_WEBHOOK=

# 创建日志目录
RUN mkdir -p /app/logs

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import requests; requests.get('${ES_HOST}/_cluster/health', auth=('${ES_USER}', '${ES_PASSWORD}'), timeout=5)" || exit 1

# 默认启动命令（可被覆盖）
CMD ["/app/entrypoint.sh"]
