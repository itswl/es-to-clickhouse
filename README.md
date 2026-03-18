# ByteHouse 数据同步工具集

将多种数据源同步到 ByteHouse（火山引擎版 ClickHouse），支持全量同步和持续增量同步。

## 支持的数据源

| 数据源 | 目录 | 说明 |
|--------|------|------|
| Elasticsearch | `es/` | ES 索引同步到 ByteHouse |
| ClickHouse | `clickhouse/` | ClickHouse 表同步到 ByteHouse |
| MongoDB | `mongodb/` | MongoDB 集合同步到 ByteHouse |

## 功能特性

- **全量同步**：首次运行时迁移全量数据
- **增量同步**：基于时间戳/ID 字段持续同步新增数据
- **auto 模式**：智能判断，首次全量后自动转增量，重启后跳过全量
- **自动字段发现**：自动扫描源数据结构并创建目标表
- **多表/多集合支持**：支持逗号分隔指定多个表或使用通配符
- **飞书告警**：WARNING/ERROR 自动推送飞书通知
- **Docker 部署**：支持 Docker Compose 一键部署

## 目录结构

```
bytehouse/
├── README.md
│
├── es/                              # Elasticsearch → ByteHouse
│   ├── es_to_bytehouse.py
│   ├── requirements.txt
│   ├── .env / .env.example
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── entrypoint.sh
│
├── clickhouse/                      # ClickHouse → ByteHouse
│   ├── clickhouse_to_bytehouse.py
│   ├── requirements.txt
│   ├── clickhouse_to_bytehouse.env
│   ├── Dockerfile_clickhouse
│   └── docker-compose.yml
│
└── mongodb/                         # MongoDB → ByteHouse
    ├── mongodb_to_bytehouse.py
    ├── requirements.txt
    ├── mongodb_to_bytehouse.env
    ├── Dockerfile_mongodb
    └── docker-compose-mongo.yaml
```

---

## MongoDB 同步

### 快速开始

```bash
cd mongodb

# 1. 配置连接信息
vim mongodb_to_bytehouse.env

# 2. Docker Compose 启动
docker-compose -f docker-compose-mongo.yaml up -d

# 3. 查看日志
docker-compose -f docker-compose-mongo.yaml logs -f
```

### 配置说明

```bash
# mongodb_to_bytehouse.env

# MongoDB 配置
MONGO_URI=mongodb://user:pass@host:port/?authSource=admin
MONGO_DATABASE=mydb

# ByteHouse 配置
TARGET_BH_HOST=tenant-xxx.bytehouse.volces.com
TARGET_BH_PORT=19000
TARGET_BH_USER=bytehouse
TARGET_BH_PASSWORD=xxx
TARGET_BH_DATABASE=mongo_sync

# 同步配置
COLLECTION_PATTERN=collection1,collection2,*_log   # 支持逗号分隔、通配符
SYNC_BATCH_SIZE=1000
INCREMENTAL_INTERVAL=60

# 告警配置
FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
```

### 命令行用法

```bash
# auto 模式（推荐）：首次全量，之后持续增量
python mongodb_to_bytehouse.py --mode auto

# 全量同步
python mongodb_to_bytehouse.py --mode full --collection-pattern "message_log"

# 增量同步
python mongodb_to_bytehouse.py --mode incremental --collection-pattern "message_log,user_log"

# 查看同步状态
python mongodb_to_bytehouse.py --mode status
```

---

## ClickHouse 同步

### 快速开始

```bash
cd clickhouse

# 1. 配置连接信息
vim clickhouse_to_bytehouse.env

# 2. Docker Compose 启动
docker-compose up -d
```

### 配置说明

```bash
# clickhouse_to_bytehouse.env

# 源 ClickHouse 配置
SOURCE_CH_HOST=192.168.1.100
SOURCE_CH_PORT=9000
SOURCE_CH_USER=default
SOURCE_CH_PASSWORD=xxx
SOURCE_CH_DATABASE=mydb

# 目标 ByteHouse 配置
TARGET_BH_HOST=tenant-xxx.bytehouse.volces.com
TARGET_BH_PORT=19000
TARGET_BH_USER=bytehouse
TARGET_BH_PASSWORD=xxx
TARGET_BH_DATABASE=ch_sync

# 同步配置
TABLE_PATTERN=spans,logs,*_events    # 支持逗号分隔、通配符
SYNC_BATCH_SIZE=10000
INCREMENTAL_INTERVAL=60
```

### 命令行用法

```bash
# 全量同步
python clickhouse_to_bytehouse.py --mode full

# 持续增量同步
python clickhouse_to_bytehouse.py --mode incremental --continuous --interval 60

# 指定时间字段和开始日期
python clickhouse_to_bytehouse.py --mode incremental --time-column "created_at" --start-date "2026-03-01"

# 查看同步状态
python clickhouse_to_bytehouse.py --mode status
```

---

## Elasticsearch 同步

### 快速开始

```bash
cd es

# 1. 配置连接信息
vim .env

# 2. Docker Compose 启动
docker-compose up -d
```

### 配置说明

```bash
# .env

# ES 配置
ES_HOST=http://es-host:9200
ES_USER=admin
ES_PASSWORD=xxx

# ByteHouse 配置
BYTEHOUSE_HOST=tenant-xxx.bytehouse.volces.com
BYTEHOUSE_PORT=19000
BYTEHOUSE_USER=bytehouse
BYTEHOUSE_PASSWORD=xxx

# 同步配置
TARGET_DATABASE=es_migration
INDEX_PATTERN=user_*,order_*         # 支持通配符
INCREMENTAL_INTERVAL=60
SKIP_FULL_SYNC=false
```

---

## 同步模式说明

| 模式 | 说明 |
|------|------|
| `full` | 全量同步，清空目标表后重新导入 |
| `incremental` | 增量同步，基于时间字段或 _id 同步新数据 |
| `auto` | 智能模式：首次全量，之后持续增量，重启后自动跳过全量 |
| `status` | 查看同步状态 |

## 表/集合匹配语法

支持逗号分隔多个模式和通配符：

```bash
# 单个
COLLECTION_PATTERN=message_log

# 多个（逗号分隔）
COLLECTION_PATTERN=message_log,user_log,order_log

# 通配符
COLLECTION_PATTERN=*_log

# 混合
COLLECTION_PATTERN=message_log,*_event,user_*
```

## 飞书告警

配置 `FEISHU_WEBHOOK` 后，所有 WARNING 和 ERROR 级别日志会自动推送到飞书：

- ⚠️ WARNING：连接重试、数据异常等
- ❌ ERROR：同步失败、连接断开等

相同错误 1 分钟内只发送一次，防止刷屏。

## 注意事项

1. **ByteHouse 表创建延迟**：表创建后需等待 5 秒才能插入数据
2. **增量字段**：MongoDB 默认使用 `_id`，ClickHouse/ES 需指定时间字段
3. **字段类型**：所有字段统一存储为 String 类型，嵌套对象转为 JSON 字符串
4. **网络访问**：确保 Docker 容器能访问源数据库和 ByteHouse

## License

MIT
