#!/usr/bin/env python3
"""
MongoDB to ByteHouse 数据同步工具
支持全量同步和增量同步
"""

import os
import sys
import time
import json
import logging
import requests
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from bson import ObjectId
from clickhouse_driver import Client

# 加载 .env 文件（如果存在）
try:
    from dotenv import load_dotenv
    load_dotenv('mongodb_to_bytehouse.env')
    load_dotenv('.env')
except ImportError:
    pass

# 配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "")


class FeishuHandler(logging.Handler):
    """飞书 Webhook 日志处理器，用于发送 WARNING 和 ERROR 级别日志"""
    
    def __init__(self, webhook_url: str):
        super().__init__()
        self.webhook_url = webhook_url
        self.last_send_time = {}
        self.send_interval = 60  # 相同错误的最小发送间隔（秒）
    
    def emit(self, record):
        try:
            # 只处理 WARNING 和 ERROR 级别
            if record.levelno < logging.WARNING:
                return
            
            # 防止相同错误频繁发送（1分钟内只发送一次）
            msg_key = f"{record.levelname}:{record.getMessage()[:100]}"
            current_time = time.time()
            if msg_key in self.last_send_time:
                if current_time - self.last_send_time[msg_key] < self.send_interval:
                    return
            self.last_send_time[msg_key] = current_time
            
            # 构建飞书消息
            level_emoji = "⚠️" if record.levelno == logging.WARNING else "❌"
            title = f"{level_emoji} MongoDB 同步告警 - {record.levelname}"
            
            content = f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            content += f"**级别**: {record.levelname}\n"
            content += f"**消息**: {record.getMessage()}\n"
            
            if record.exc_info:
                import traceback
                content += f"**异常**: {traceback.format_exception(*record.exc_info)[:500]}\n"
            
            payload = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": title},
                        "template": "orange" if record.levelno == logging.WARNING else "red"
                    },
                    "elements": [{"tag": "markdown", "content": content}]
                }
            }
            
            requests.post(self.webhook_url, json=payload, timeout=5)
        except Exception:
            pass  # 告警发送失败不影响主程序


# 配置日志
numeric_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(level=numeric_level, format='%(asctime)s [%(levelname)s] %(message)s')

logger = logging.getLogger(__name__)

# 添加文件处理器（如果指定了日志文件且文件路径存在）
if LOG_FILE:
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir and os.path.exists(log_dir):
            file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
            file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"无法创建日志文件处理器: {e}")

# 添加标准输出处理器
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# 添加飞书告警处理器
if FEISHU_WEBHOOK:
    feishu_handler = FeishuHandler(FEISHU_WEBHOOK)
    feishu_handler.setLevel(logging.WARNING)
    feishu_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(feishu_handler)
    logger.info(f"✓ 飞书告警已启用")


# MongoDB 配置
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "")
MONGO_AUTH_SOURCE = os.getenv("MONGO_AUTH_SOURCE", "admin")

# 目标 ByteHouse 配置
TARGET_HOST = os.getenv("TARGET_BH_HOST", "")
TARGET_PORT = int(os.getenv("TARGET_BH_PORT", "19000"))
TARGET_USER = os.getenv("TARGET_BH_USER", "bytehouse")
TARGET_PASSWORD = os.getenv("TARGET_BH_PASSWORD", "")
TARGET_DATABASE = os.getenv("TARGET_BH_DATABASE", "default")

# 同步配置
BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "1000"))
INCREMENTAL_INTERVAL = int(os.getenv("INCREMENTAL_INTERVAL", "60"))
COLLECTION_PATTERN = os.getenv("COLLECTION_PATTERN", "*")  # 支持逗号分隔多个模式
STORE_SOURCE = os.getenv("STORE_SOURCE", "false").lower() in ("true", "1", "yes")  # 是否存储 _source
ADD_TIMESTAMP = os.getenv("ADD_TIMESTAMP", "false").lower() in ("true", "1", "yes")  # 是否添加 _timestamp


def match_collections(collections: List[str], pattern: str) -> List[str]:
    """匹配集合名，支持逗号分隔的多个模式"""
    import fnmatch
    if pattern == "*":
        return collections
    
    # 分割多个模式（逗号分隔）
    patterns = [p.strip() for p in pattern.split(",") if p.strip()]
    
    matched = set()
    for p in patterns:
        for c in collections:
            if fnmatch.fnmatch(c, p) or c == p:
                matched.add(c)
    
    return list(matched)


class MongoDBToByteHouseSync:
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.target_client = None
        self.current_table_columns = set()

    def connect_mongodb(self):
        """连接 MongoDB"""
        try:
            from pymongo import MongoClient
            
            self.mongo_client = MongoClient(MONGO_URI, authSource=MONGO_AUTH_SOURCE)
            # 测试连接
            self.mongo_client.admin.command('ping')
            
            if MONGO_DATABASE:
                self.mongo_db = self.mongo_client[MONGO_DATABASE]
            
            logger.info(f"✓ MongoDB 连接成功: {MONGO_URI}")
            return True
        except Exception as e:
            logger.error(f"✗ MongoDB 连接失败: {e}")
            return False

    def connect_target(self):
        """连接目标 ByteHouse"""
        try:
            # 先检查并创建数据库
            temp_client = Client(
                host=TARGET_HOST,
                port=TARGET_PORT,
                user=TARGET_USER,
                password=TARGET_PASSWORD,
                secure=True
            )
            try:
                temp_client.execute(f"CREATE DATABASE IF NOT EXISTS `{TARGET_DATABASE}`")
                logger.info(f"✓ 目标数据库 {TARGET_DATABASE} 准备就绪")
            except Exception as e:
                logger.warning(f"创建目标数据库失败（可能权限不足）: {e}")
            temp_client.disconnect()
            
            # 使用指定数据库创建连接
            self.target_client = Client(
                host=TARGET_HOST,
                port=TARGET_PORT,
                user=TARGET_USER,
                password=TARGET_PASSWORD,
                database=TARGET_DATABASE,
                secure=True
            )
            # 测试连接
            self.target_client.execute("SELECT 1")
            
            # 创建同步状态表
            self.create_sync_state_table()
            
            logger.info(f"✓ 目标 ByteHouse 连接成功: {TARGET_HOST}:{TARGET_PORT}")
            return True
        except Exception as e:
            logger.error(f"✗ 目标 ByteHouse 连接失败: {e}")
            return False

    def create_sync_state_table(self):
        """创建同步状态表"""
        try:
            sql = f"""
CREATE TABLE IF NOT EXISTS `_sync_state` (
    `table_name` String,
    `last_sync_time` String,
    `last_id` String DEFAULT '',
    `sync_count` UInt64,
    `sync_time` DateTime DEFAULT now()
) ENGINE = CnchMergeTree()
ORDER BY (`table_name`, `sync_time`)
SETTINGS index_granularity = 8192
"""
            self.target_client.execute(sql)
            logger.info("✓ 同步状态表 _sync_state 准备就绪")
        except Exception as e:
            if "already exists" not in str(e).lower():
                logger.error(f"✗ 创建同步状态表失败: {e}")

    def get_last_sync_state(self, collection_name: str) -> Tuple[str, str]:
        """获取集合的最后同步状态（最后ID和最后时间）"""
        try:
            result = self.target_client.execute(
                f"SELECT max(last_id), max(last_sync_time) FROM `_sync_state` WHERE `table_name` = '{collection_name}'"
            )
            if result and result[0]:
                last_id = result[0][0] if result[0][0] else ""
                last_time = result[0][1] if result[0][1] else ""
                return last_id, last_time
        except Exception as e:
            logger.warning(f"获取同步状态失败: {e}")
        
        return "", ""

    def update_sync_state(self, collection_name: str, last_id: str, last_time: str = "", sync_count: int = 0):
        """更新同步状态"""
        try:
            self.target_client.execute(
                f"INSERT INTO `_sync_state` (`table_name`, `last_id`, `last_sync_time`, `sync_count`) VALUES",
                [(collection_name, last_id, last_time, sync_count)]
            )
            logger.debug(f"同步状态已更新: {collection_name}, last_id={last_id}")
        except Exception as e:
            logger.error(f"更新同步状态失败: {e}")

    def query_sync_state(self, collection_name: str = ""):
        """查询同步状态"""
        try:
            if collection_name:
                result = self.target_client.execute(
                    f"SELECT `table_name`, `last_id`, `last_sync_time`, `sync_count`, `sync_time` FROM `_sync_state` WHERE `table_name` = '{collection_name}' ORDER BY `sync_time` DESC LIMIT 10"
                )
            else:
                result = self.target_client.execute(
                    f"SELECT `table_name`, `last_id`, `last_sync_time`, `sync_count`, `sync_time` FROM `_sync_state` ORDER BY `sync_time` DESC LIMIT 100"
                )
            
            logger.info(f"同步状态查询结果 (共 {len(result)} 条):")
            for row in result:
                logger.info(f"  集合: {row[0]}, 最后ID: {row[1][:20]}..., 最后时间: {row[2]}, 同步数量: {row[3]}, 状态时间: {row[4]}")
                
            return result
        except Exception as e:
            logger.error(f"查询同步状态失败: {e}")
            return []

    def has_synced_before(self, collection_name: str) -> bool:
        """检查集合是否已经同步过（用于判断是否跳过全量）"""
        try:
            table_name = collection_name.replace("-", "_").replace(".", "_")
            result = self.target_client.execute(
                f"SELECT count() FROM `_sync_state` WHERE `table_name` = '{table_name}'"
            )
            return result and result[0][0] > 0
        except Exception as e:
            logger.debug(f"检查同步状态失败: {e}")
            return False

    def get_collections(self) -> List[str]:
        """获取 MongoDB 数据库中的集合列表"""
        if self.mongo_db is None:
            return []
        
        collections = self.mongo_db.list_collection_names()
        # 过滤系统集合
        return [c for c in collections if not c.startswith('system.')]

    def flatten_document(self, doc: Dict, prefix: str = "") -> Dict[str, Any]:
        """将文档转换为可插入的格式，不展开嵌套字段"""
        result = {}
        for key, value in doc.items():
            # 清理键名
            clean_key = key.replace(".", "_").replace("-", "_").replace("$", "_")
            
            if isinstance(value, dict):
                # 嵌套对象直接转为 JSON 字符串
                result[clean_key] = json.dumps(value, ensure_ascii=False, default=str)
            elif isinstance(value, list):
                # 列表转为 JSON 字符串
                result[clean_key] = json.dumps(value, ensure_ascii=False, default=str)
            elif isinstance(value, ObjectId):
                # ObjectId 转为字符串
                result[clean_key] = str(value)
            elif isinstance(value, datetime):
                # datetime 转为 ISO 格式字符串
                result[clean_key] = value.isoformat()
            elif isinstance(value, bytes):
                # bytes 转为 hex 字符串
                result[clean_key] = value.hex()
            elif value is None:
                result[clean_key] = None
            else:
                # 其他值转为字符串
                result[clean_key] = str(value)
        return result

    def infer_type(self, value) -> str:
        """推断字段类型"""
        if value is None:
            return "String"
        if isinstance(value, bool):
            return "Bool"
        if isinstance(value, int):
            return "Int64"
        if isinstance(value, float):
            return "Float64"
        return "String"

    def scan_collection_fields(self, collection_name: str, sample_size: int = 1000) -> Dict[str, str]:
        """扫描集合样本数据获取所有字段"""
        fields = {}
        collection = self.mongo_db[collection_name]
        
        # 随机抽样
        docs = collection.aggregate([{"$sample": {"size": sample_size}}])
        
        for doc in docs:
            flat_doc = self.flatten_document(doc)
            for field_name, value in flat_doc.items():
                if field_name not in fields:
                    fields[field_name] = self.infer_type(value)
        
        return fields

    def create_target_table(self, table_name: str, fields: Dict[str, str]) -> bool:
        """在目标 ByteHouse 创建表"""
        try:
            # 构建列定义
            columns = []
            columns.append("`_id` String")  # MongoDB 的 _id 字段
            if STORE_SOURCE:
                columns.append("`_source` String")  # 原始 JSON
            if ADD_TIMESTAMP:
                columns.append("`_timestamp` DateTime DEFAULT now()")  # 同步时间戳
            
            for field_name, field_type in sorted(fields.items()):
                if field_name in ("_id", "_source", "_timestamp"):
                    continue
                columns.append(f"`{field_name}` Nullable(String)")
            
            columns_str = ",\n".join(columns)
            sql = f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
{columns_str}
) ENGINE = CnchMergeTree()
ORDER BY tuple()
UNIQUE KEY _id
SETTINGS index_granularity = 8192
"""
            self.target_client.execute(sql)
            logger.info(f"✓ 表 {TARGET_DATABASE}.{table_name} 创建成功")
            logger.info(f"    字段数量: {len(fields) + 3}")
            return True
        except Exception as e:
            logger.error(f"✗ 创建表 {table_name} 失败: {e}")
            return False

    def load_table_columns(self, table_name: str):
        """加载目标表的列信息"""
        try:
            columns = self.target_client.execute(f"DESCRIBE TABLE `{table_name}`")
            self.current_table_columns = {col[0] for col in columns}
            logger.debug(f"  表 {table_name} 有 {len(self.current_table_columns)} 个列")
        except Exception as e:
            logger.warning(f"  加载表结构失败: {e}")
            self.current_table_columns = set()

    def add_new_columns(self, table_name: str, new_fields: Dict[str, str]) -> int:
        """动态添加新列到已有表"""
        added_count = 0
        for field_name, field_type in new_fields.items():
            if field_name in self.current_table_columns:
                continue
            if field_name in ("_id", "_source", "_timestamp"):
                continue
            
            try:
                sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{field_name}` Nullable(String)"
                self.target_client.execute(sql)
                self.current_table_columns.add(field_name)
                added_count += 1
                logger.debug(f"    添加新列: {field_name}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.debug(f"    添加列 {field_name} 失败: {e}")
        
        return added_count

    def detect_new_fields_from_batch(self, batch: List[Dict]) -> Dict[str, str]:
        """从批次数据中检测新字段"""
        new_fields = {}
        for row in batch:
            for field_name, value in row.items():
                if field_name not in self.current_table_columns and field_name not in new_fields:
                    if field_name not in ("_id", "_source", "_timestamp"):
                        new_fields[field_name] = self.infer_type(value)
        return new_fields

    def insert_batch(self, table_name: str, batch: List[Dict]):
        """批量插入数据到 ByteHouse"""
        if not batch:
            return
        
        # 只使用表中定义的列，排除自动填充的列
        exclude_cols = set()
        if ADD_TIMESTAMP:
            exclude_cols.add("_timestamp")
        if not STORE_SOURCE:
            exclude_cols.add("_source")
        columns = sorted(list(self.current_table_columns - exclude_cols))
        
        # 构建插入数据
        rows = []
        for row in batch:
            row_data = tuple(row.get(col) for col in columns)
            rows.append(row_data)
        
        # 构建 INSERT 语句
        columns_str = ", ".join(f"`{col}`" for col in columns)
        sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES"
        
        try:
            self.target_client.execute(sql, rows, types_check=True)
        except Exception as e:
            error_msg = str(e)
            if "No such column" in error_msg or "SQLSTATE: 42703" in error_msg:
                logger.warning(f"    插入失败，尝试添加缺失列: {error_msg[:100]}")
                new_fields = self.detect_new_fields_from_batch(batch)
                if new_fields:
                    added = self.add_new_columns(table_name, new_fields)
                    logger.info(f"    添加了 {added} 个缺失的列，重试插入")
                    # 重新构建并重试
                    exclude_cols = set()
                    if ADD_TIMESTAMP:
                        exclude_cols.add("_timestamp")
                    if not STORE_SOURCE:
                        exclude_cols.add("_source")
                    columns = sorted(list(self.current_table_columns - exclude_cols))
                    rows = [tuple(row.get(col) for col in columns) for row in batch]
                    columns_str = ", ".join(f"`{col}`" for col in columns)
                    sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES"
                    try:
                        self.target_client.execute(sql, rows, types_check=True)
                        return
                    except Exception as e2:
                        logger.error(f"    重试插入仍然失败: {e2}")
            else:
                logger.warning(f"    批量插入失败: {e}")

    def sync_collection_full(self, collection_name: str) -> bool:
        """全量同步单个集合"""
        logger.info(f"开始全量同步集合: {collection_name}")
        
        collection = self.mongo_db[collection_name]
        table_name = collection_name.replace("-", "_").replace(".", "_")
        
        # 获取文档总数
        total_docs = collection.estimated_document_count()
        logger.info(f"  文档总数: {total_docs:,}")
        
        if total_docs == 0:
            logger.info("  集合为空，跳过同步")
            return True
        
        # 扫描字段
        fields = self.scan_collection_fields(collection_name)
        logger.info(f"  扫描到 {len(fields)} 个字段")
        
        # 创建目标表
        if not self.create_target_table(table_name, fields):
            return False
        
        # 等待表在分布式环境中完全可用
        import time
        logger.info("  等待表在分布式环境中就绪...")
        time.sleep(5)
        
        # 加载表列信息
        self.load_table_columns(table_name)
        
        # 分批同步数据
        total_synced = 0
        start_time = time.time()
        last_id = None
        
        while True:
            # 构建查询条件
            if last_id:
                query = {"_id": {"$gt": last_id}}
            else:
                query = {}
            
            # 查询批次数据
            cursor = collection.find(query).sort("_id", 1).limit(BATCH_SIZE)
            docs = list(cursor)
            
            if not docs:
                break
            
            # 处理文档
            batch = []
            for doc in docs:
                row = {"_id": str(doc.get("_id", ""))}
                if STORE_SOURCE:
                    row["_source"] = json.dumps(doc, ensure_ascii=False, default=str)
                
                flat_doc = self.flatten_document(doc)
                row.update(flat_doc)
                batch.append(row)
                last_id = doc.get("_id")
            
            # 检测并添加新字段
            new_fields = self.detect_new_fields_from_batch(batch)
            if new_fields:
                added = self.add_new_columns(table_name, new_fields)
                if added > 0:
                    logger.info(f"    发现并添加 {added} 个新字段")
            
            # 插入数据
            self.insert_batch(table_name, batch)
            total_synced += len(batch)
            
            # 打印进度
            elapsed = time.time() - start_time
            speed = total_synced / elapsed if elapsed > 0 else 0
            progress = (total_synced / total_docs * 100) if total_docs > 0 else 0
            logger.info(f"    进度: {total_synced:,}/{total_docs:,} ({progress:.1f}%) | 速度: {speed:.0f}/s")
        
        # 更新同步状态
        if last_id:
            self.update_sync_state(collection_name, str(last_id), datetime.now().isoformat(), total_synced)
        
        elapsed = time.time() - start_time
        logger.info(f"✓ 集合 {collection_name} 全量同步完成")
        logger.info(f"    同步文档数: {total_synced:,}")
        logger.info(f"    耗时: {elapsed:.1f}s")
        logger.info(f"    平均速度: {total_synced/elapsed:.0f}/s" if elapsed > 0 else "")
        
        return True

    def sync_collection_incremental(self, collection_name: str, time_column: str = "", start_date: str = "") -> int:
        """增量同步单个集合"""
        logger.info(f"开始增量同步集合: {collection_name}")
        
        collection = self.mongo_db[collection_name]
        table_name = collection_name.replace("-", "_").replace(".", "_")
        
        # 加载表列信息
        self.load_table_columns(table_name)
        
        # 如果表不存在，需要先创建表
        if not self.current_table_columns:
            logger.info(f"  目标表不存在，先扫描字段并创建表")
            # 采样扫描字段
            sample_docs = list(collection.find().limit(100))
            if not sample_docs:
                logger.warning(f"  集合 {collection_name} 为空，跳过")
                return 0
            fields = {}
            for doc in sample_docs:
                flat_doc = self.flatten_document(doc)
                for field_name, value in flat_doc.items():
                    if field_name not in fields:
                        fields[field_name] = self.infer_type(value)
            # 创建表
            if not self.create_target_table(table_name, fields):
                logger.error(f"  创建表失败，跳过集合 {collection_name}")
                return 0
        
        # 获取上次同步状态
        last_id, last_time = self.get_last_sync_state(collection_name)
        
        # 构建查询条件
        query = {}
        if start_date and time_column:
            # 如果指定了开始日期和时间字段
            try:
                start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                query[time_column] = {"$gt": start_dt}
                logger.info(f"  使用时间条件: {time_column} > {start_date}")
            except:
                logger.warning(f"  无法解析开始日期: {start_date}，使用 _id 增量")
                if last_id:
                    query["_id"] = {"$gt": ObjectId(last_id)}
        elif last_id:
            # 使用上次同步的 _id
            try:
                query["_id"] = {"$gt": ObjectId(last_id)}
                logger.info(f"  使用 _id 增量条件: _id > {last_id}")
            except:
                logger.warning(f"  无法解析 last_id: {last_id}")
        else:
            logger.info(f"  首次增量同步，同步所有数据")
        
        # 查询数据
        cursor = collection.find(query).sort("_id", 1)
        
        total_synced = 0
        current_last_id = last_id
        batch = []
        
        for doc in cursor:
            row = {"_id": str(doc.get("_id", ""))}
            if STORE_SOURCE:
                row["_source"] = json.dumps(doc, ensure_ascii=False, default=str)
            
            flat_doc = self.flatten_document(doc)
            row.update(flat_doc)
            batch.append(row)
            current_last_id = str(doc.get("_id"))
            
            if len(batch) >= BATCH_SIZE:
                # 检测并添加新字段
                new_fields = self.detect_new_fields_from_batch(batch)
                if new_fields:
                    added = self.add_new_columns(table_name, new_fields)
                    if added > 0:
                        logger.info(f"    发现并添加 {added} 个新字段")
                
                self.insert_batch(table_name, batch)
                total_synced += len(batch)
                logger.info(f"    已同步 {total_synced:,} 条")
                batch = []
        
        # 处理剩余数据
        if batch:
            new_fields = self.detect_new_fields_from_batch(batch)
            if new_fields:
                added = self.add_new_columns(table_name, new_fields)
                if added > 0:
                    logger.info(f"    发现并添加 {added} 个新字段")
            
            self.insert_batch(table_name, batch)
            total_synced += len(batch)
        
        # 更新同步状态
        if total_synced > 0 and current_last_id:
            self.update_sync_state(collection_name, current_last_id, datetime.now().isoformat(), total_synced)
        
        logger.info(f"  增量同步完成: {total_synced} 条")
        return total_synced

    def run_full_sync(self, collection_pattern: str = "*"):
        """运行全量同步"""
        logger.info("="*60)
        logger.info("MongoDB to ByteHouse 全量同步")
        logger.info("="*60)
        logger.info(f"源数据库: {MONGO_DATABASE}")
        logger.info(f"目标数据库: {TARGET_DATABASE}")
        logger.info(f"集合模式: {collection_pattern}")
        logger.info(f"批次大小: {BATCH_SIZE}")
        
        if not self.connect_mongodb():
            return
        
        if not self.connect_target():
            return
        
        # 获取集合列表
        collections = self.get_collections()
        if collection_pattern != "*":
            collections = match_collections(collections, collection_pattern)
        
        if not collections:
            logger.warning("没有找到要同步的集合")
            return
        
        logger.info(f"")
        logger.info(f"找到 {len(collections)} 个集合:")
        for c in collections:
            logger.info(f"  - {c}")
        
        # 同步每个集合
        success_count = 0
        for collection_name in collections:
            try:
                if self.sync_collection_full(collection_name):
                    success_count += 1
            except Exception as e:
                logger.error(f"✗ 同步集合 {collection_name} 时发生错误: {e}")
        
        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"全量同步完成: 成功 {success_count}/{len(collections)} 个集合")
        logger.info(f"{'='*60}")

    def run_incremental(self, collection_pattern: str = "*", time_column: str = "", 
                       continuous: bool = False, interval: int = INCREMENTAL_INTERVAL, start_date: str = ""):
        """运行增量同步"""
        logger.info("="*60)
        logger.info("MongoDB to ByteHouse 增量同步")
        logger.info("="*60)
        logger.info(f"模式: {'持续同步' if continuous else '单次同步'}")
        if continuous:
            logger.info(f"同步间隔: {interval} 秒")
        logger.info(f"集合模式: {collection_pattern}")
        if time_column:
            logger.info(f"时间字段: {time_column}")
        if start_date:
            logger.info(f"开始日期: {start_date}")
        
        if not self.connect_mongodb():
            return
        
        if not self.connect_target():
            return
        
        # 获取集合列表
        collections = self.get_collections()
        if collection_pattern != "*":
            collections = match_collections(collections, collection_pattern)
        
        if not collections:
            logger.warning("没有找到要同步的集合")
            return
        
        round_count = 0
        while True:
            round_count += 1
            logger.info(f"")
            logger.info(f"[第 {round_count} 轮] 开始增量同步 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            
            total_synced = 0
            for collection_name in collections:
                try:
                    synced = self.sync_collection_incremental(collection_name, time_column, start_date)
                    total_synced += synced
                except Exception as e:
                    logger.error(f"  ✗ 同步集合 {collection_name} 失败: {e}")
            
            logger.info(f"")
            logger.info(f"[第 {round_count} 轮] 同步完成，本轮共同步 {total_synced:,} 条")
            
            if not continuous:
                break
            
            logger.info(f"等待 {interval} 秒后进行下一轮...")
            time.sleep(interval)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="MongoDB to ByteHouse 数据同步工具")
    parser.add_argument("--mode", choices=["full", "incremental", "auto", "status"], default="full", 
                       help="同步模式: full(全量), incremental(增量), auto(先全量后增量) 或 status(查看同步状态)")
    parser.add_argument("--collection-pattern", default=COLLECTION_PATTERN,
                       help="集合名模式，支持逗号分隔多个，如 'col1,col2,log_*'，默认从 COLLECTION_PATTERN 环境变量读取")
    parser.add_argument("--time-column", default="", 
                       help="增量同步使用的时间字段（可选，默认使用 _id）")
    parser.add_argument("--start-date", default="", 
                       help="开始日期，格式: YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--continuous", action="store_true", 
                       help="持续增量同步模式")
    parser.add_argument("--interval", type=int, default=INCREMENTAL_INTERVAL,
                       help="增量同步间隔（秒）")
    
    args = parser.parse_args()
    
    sync = MongoDBToByteHouseSync()
    
    if args.mode == "full":
        sync.run_full_sync(args.collection_pattern)
    elif args.mode == "incremental":
        sync.run_incremental(
            collection_pattern=args.collection_pattern,
            time_column=args.time_column,
            continuous=args.continuous,
            interval=args.interval,
            start_date=args.start_date
        )
    elif args.mode == "auto":
        # 自动模式：检查是否已同步过，如已同步则直接增量
        logger.info("============================================================")
        logger.info("自动模式：检测同步状态后决定全量或增量")
        logger.info("============================================================")
        
        # 连接并检查是否已同步过
        if not sync.connect_target():
            logger.error("无法连接目标数据库")
            return
        
        # 获取需要同步的集合列表
        if not sync.connect_mongodb():
            logger.error("无法连接 MongoDB")
            return
        
        collections = sync.get_collections()
        if args.collection_pattern != "*":
            collections = match_collections(collections, args.collection_pattern)
        
        # 检查是否所有集合都已同步过
        need_full_sync = False
        for collection_name in collections:
            table_name = collection_name.replace("-", "_").replace(".", "_")
            if not sync.has_synced_before(table_name):
                logger.info(f"  集合 {collection_name} 未同步过，需要全量同步")
                need_full_sync = True
            else:
                logger.info(f"  集合 {collection_name} 已有同步记录，将跳过全量")
        
        if need_full_sync:
            # 执行全量同步
            logger.info("")
            logger.info("============================================================")
            logger.info("执行全量同步...")
            logger.info("============================================================")
            sync.run_full_sync(args.collection_pattern)
        else:
            logger.info("")
            logger.info("============================================================")
            logger.info("检测到已有同步记录，跳过全量同步")
            logger.info("============================================================")
        
        # 转入持续增量同步
        logger.info("")
        logger.info("============================================================")
        logger.info("转入持续增量同步模式...")
        logger.info("============================================================")
        
        sync.run_incremental(
            collection_pattern=args.collection_pattern,
            time_column=args.time_column,
            continuous=True,  # 自动模式下强制持续增量
            interval=args.interval,
            start_date=""  # 从上次同步位置继续
        )
    elif args.mode == "status":
        if sync.connect_target():
            sync.query_sync_state(args.collection_pattern if args.collection_pattern != "*" else "")


if __name__ == "__main__":
    main()
