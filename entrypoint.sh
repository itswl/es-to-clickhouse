#!/bin/bash
# ES to ByteHouse 同步启动脚本
# 先执行全量同步，然后持续增量同步

set -e

echo "=============================================="
echo "ES to ByteHouse 数据同步服务"
echo "=============================================="
echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 显示配置信息
echo "配置信息:"
echo "  ES_HOST: ${ES_HOST}"
echo "  BYTEHOUSE_HOST: ${BYTEHOUSE_HOST}"
echo "  TARGET_DATABASE: ${TARGET_DATABASE}"
echo "  INDEX_PATTERN: ${INDEX_PATTERN}"
echo "  INCREMENTAL_INTERVAL: ${INCREMENTAL_INTERVAL}s"
echo ""

# 检查是否需要跳过全量同步
if [ "${SKIP_FULL_SYNC}" = "true" ]; then
    echo "跳过全量同步（SKIP_FULL_SYNC=true）"
else
    echo "=============================================="
    echo "步骤 1: 执行全量同步"
    echo "=============================================="
    python /app/es_to_bytehouse.py --mode full --pattern "${INDEX_PATTERN}"
    
    if [ $? -ne 0 ]; then
        echo "全量同步失败，退出"
        exit 1
    fi
    
    echo ""
    echo "全量同步完成"
    echo ""
fi

echo "=============================================="
echo "步骤 2: 启动持续增量同步"
echo "=============================================="
echo "增量同步间隔: ${INCREMENTAL_INTERVAL} 秒"
echo ""

# 启动持续增量同步
exec python /app/es_to_bytehouse.py --mode continuous --pattern "${INDEX_PATTERN}" --interval "${INCREMENTAL_INTERVAL}"
