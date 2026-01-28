# ES to ByteHouse 数据同步工具

将 Elasticsearch 数据同步到 ByteHouse（火山引擎版 ClickHouse），支持全量同步和持续增量同步。

## 功能特性

- **全量同步**：首次运行时，扫描 ES 索引的全量数据并迁移到 ByteHouse
- **增量同步**：基于时间戳字段，持续同步新增数据
- **自动字段发现**：自动扫描 ES 数据，发现所有字段并创建对应的表结构
- **Docker 部署**：支持 Docker/Docker Compose 一键部署
- **配置灵活**：通过环境变量配置，无需修改代码

## 快速开始

### 1. 配置连接信息

编辑 `.env` 文件：

```bash
# Elasticsearch 配置
ES_HOST=http://your-es-host:9200
ES_USER=admin
ES_PASSWORD=your-password

# ByteHouse 配置
BYTEHOUSE_HOST=your-bytehouse-host.bytehouse.volces.com
BYTEHOUSE_PORT=19000
BYTEHOUSE_USER=bytehouse
BYTEHOUSE_PASSWORD=your-password
BYTEHOUSE_SECURE=true

# 同步配置
TARGET_DATABASE=es_migration
INDEX_PATTERN=*
INCREMENTAL_INTERVAL=60
```

### 2. Docker Compose 启动（推荐）

```bash
# 启动（全量 + 持续增量）
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down
```

### 3. Docker 直接运行

```bash
# 构建镜像
docker build -t es-bytehouse-sync .

# 运行（全量 + 持续增量）
docker run -d --name es-sync --env-file .env es-bytehouse-sync

# 仅增量同步（跳过全量）
docker run -d --name es-sync --env-file .env -e SKIP_FULL_SYNC=true es-bytehouse-sync

# 同步指定索引
docker run -d --name es-sync --env-file .env -e INDEX_PATTERN=user_info_prod es-bytehouse-sync
```

### 4. 本地运行（不使用 Docker）

```bash
# 安装依赖
pip install -r requirements.txt

# 全量同步
python es_to_bytehouse.py --mode full

# 单次增量同步
python es_to_bytehouse.py --mode incremental

# 持续增量同步
python es_to_bytehouse.py --mode continuous --interval 60

# 同步指定索引
python es_to_bytehouse.py --mode full --index user_info_prod

# 列出所有索引
python es_to_bytehouse.py --list-only
```

## 环境变量说明

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `ES_HOST` | - | Elasticsearch 地址 |
| `ES_USER` | admin | ES 用户名 |
| `ES_PASSWORD` | - | ES 密码 |
| `BYTEHOUSE_HOST` | - | ByteHouse 地址 |
| `BYTEHOUSE_PORT` | 19000 | ByteHouse 端口 |
| `BYTEHOUSE_USER` | bytehouse | ByteHouse 用户名 |
| `BYTEHOUSE_PASSWORD` | - | ByteHouse 密码 |
| `BYTEHOUSE_SECURE` | true | 是否使用 SSL |
| `TARGET_DATABASE` | es_migration | 目标数据库名 |
| `INDEX_PATTERN` | * | 要同步的索引模式，支持通配符 |
| `INCREMENTAL_INTERVAL` | 60 | 增量同步间隔（秒）|
| `SKIP_FULL_SYNC` | false | 是否跳过全量同步 |
| `STORE_SOURCE` | false | 是否存储原始 _source JSON |
| `BATCH_SIZE` | 1000 | 每批插入的数据量 |
| `SCROLL_SIZE` | 1000 | ES 每次拉取的数据量 |
| `LOG_LEVEL` | INFO | 日志级别: DEBUG, INFO, WARNING, ERROR |

## 日志说明

日志输出格式: `时间 [级别] 消息`

**日志级别**:
- `DEBUG`: 最详细，包含 SQL 语句、字段扫描进度等
- `INFO`: 标准级别，包含同步进度、耗时统计等
- `WARNING`: 警告信息，如缺少时间字段
- `ERROR`: 错误信息

**示例日志**:
```
2026-01-27 18:22:45 [INFO] ============================================================
2026-01-27 18:22:45 [INFO] ES to ByteHouse 增量同步
2026-01-27 18:22:45 [INFO] ============================================================
2026-01-27 18:22:45 [INFO] 模式: 单次同步
2026-01-27 18:22:45 [INFO] 索引模式: user_info_prod
2026-01-27 18:22:45 [INFO] 正在连接 Elasticsearch: http://your-es-host:9200
2026-01-27 18:22:45 [INFO] ✓ ES 连接成功
2026-01-27 18:22:45 [INFO]   集群名称: my-cluster
2026-01-27 18:22:45 [INFO]   集群状态: green
2026-01-27 18:22:45 [INFO]   节点数量: 7
2026-01-27 18:22:46 [INFO] ✓ ByteHouse 连接成功
2026-01-27 18:22:46 [INFO] [第 1 轮] 开始增量同步 (2026-01-27 18:22:46)
2026-01-27 18:22:46 [INFO] 检查索引: user_info_prod
2026-01-27 18:22:46 [INFO]   发现 19 条增量数据
2026-01-27 18:22:46 [INFO]   已同步 19/19 条
2026-01-27 18:22:47 [INFO]   ✓ 增量同步完成: 19 条，耗时 0.8s
2026-01-27 18:22:47 [INFO] [第 1 轮] 同步完成，本轮共同步 19 条
```

## 数据结构

同步后的表结构：

- `_id`: ES 文档 ID
- `_source`: 原始 JSON 文档（完整保留）
- `_timestamp`: 同步时间戳
- 其他字段: ES 文档的展平字段（嵌套字段用下划线连接）

## 查询示例

```sql
-- 查看记录数
SELECT count() FROM es_migration.user_info_prod;

-- 查询展平字段
SELECT _id, user_id, username, first_chat_time 
FROM es_migration.user_info_prod 
LIMIT 10;

-- 查看同步状态
SELECT * FROM es_migration._sync_state ORDER BY updated_at DESC;
```

## 文件结构

```
├── .env                  # 配置文件
├── .dockerignore         # Docker 忽略文件
├── Dockerfile            # Docker 镜像定义
├── docker-compose.yml    # Docker Compose 配置
├── entrypoint.sh         # Docker 启动脚本
├── es_to_bytehouse.py    # 同步脚本
├── requirements.txt      # Python 依赖
└── README.md             # 说明文档
```

## 注意事项

1. **时间字段**：增量同步依赖时间字段（如 `timestamp`、`@timestamp`），确保 ES 数据中有时间字段
2. **字段类型**：所有字段统一存储为 String 类型，避免类型转换问题
3. **大数据量**：对于超大索引（如 TB 级），建议分批同步或增大 `BATCH_SIZE`
4. **网络**：确保 Docker 容器能访问 ES 和 ByteHouse 的网络

## License

MIT
