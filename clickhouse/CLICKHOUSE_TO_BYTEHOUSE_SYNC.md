# ClickHouse to ByteHouse 数据同步工具

一个用于将数据从 ClickHouse 同步到 ByteHouse（火山引擎版 ClickHouse）的工具。

## 功能特性

- **全量同步**: 一次性同步所有数据
- **增量同步**: 基于时间字段的增量数据同步
- **表结构自动复制**: 自动创建目标表结构
- **批量处理**: 支持大批量数据高效传输
- **日志记录**: 详细的同步进度和错误日志
- **飞书告警**: 支持错误告警通知

## 配置

编辑 `clickhouse_to_bytehouse.env` 文件，配置以下参数：

```bash
# 源 ClickHouse 配置
SOURCE_CH_HOST=your-source-clickhouse-host.com
SOURCE_CH_PORT=9000
SOURCE_CH_USER=default
SOURCE_CH_PASSWORD=your-source-password
SOURCE_CH_DATABASE=default

# 目标 ByteHouse 配置  
TARGET_BH_HOST=tenant-2101986858-cn-shanghai-public.bytehouse.volces.com
TARGET_BH_PORT=19000
TARGET_BH_USER=bytehouse
TARGET_BH_PASSWORD=u31qunsnS7:kGc9w1Qzem
TARGET_BH_DATABASE=eve_cn_prod_es

# 同步配置
SYNC_BATCH_SIZE=1000          # 批量同步大小
INCREMENTAL_INTERVAL=60       # 增量同步间隔（秒）

# 日志和告警
LOG_LEVEL=INFO
LOG_FILE=/app/logs/clickhouse_to_bytehouse.log
FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/...
```

## 使用方法

### 1. 构建 Docker 镜像

```bash
docker build -f Dockerfile_clickhouse -t clickhouse-bytehouse-sync .
```

### 2. 全量同步

```bash
# 同步所有表
docker run --env-file clickhouse_to_bytehouse.env clickhouse-bytehouse-sync --mode full

# 同步特定表模式
docker run --env-file clickhouse_to_bytehouse.env clickhouse-bytehouse-sync --mode full --table-pattern "user_*"
```

### 3. 增量同步

```bash
# 单次增量同步
docker run --env-file clickhouse_to_bytehouse.env clickhouse-bytehouse-sync --mode incremental --time-column created_at

# 持续增量同步
docker run --env-file clickhouse_to_bytehouse.env clickhouse-bytehouse-sync --mode incremental --time-column created_at --continuous --interval 30
```

## 参数说明

- `--mode`: 同步模式 (`full` 或 `incremental`)
- `--table-pattern`: 表名匹配模式（支持通配符）
- `--time-column`: 增量同步使用的时间字段
- `--continuous`: 持续增量同步模式
- `--interval`: 同步间隔（秒）

## 注意事项

1. 确保源 ClickHouse 和目标 ByteHouse 都可以访问
2. 配置正确的用户名和密码
3. 根据数据量调整 `SYNC_BATCH_SIZE` 以获得最佳性能
4. 增量同步依赖于时间字段，请确保表中有合适的时间戳字段