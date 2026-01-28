#!/usr/bin/env python3
"""
ES to ByteHouse 数据迁移脚本
参考: https://clickhouse.com/docs/use-cases/observability/clickstack/migration/elastic/migrating-data

将 Elasticsearch 数据迁移到 ByteHouse (火山引擎版 ClickHouse)
支持全量同步和增量同步
"""

import os
import sys
import json
import time
import logging
import requests
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from clickhouse_driver import Client

# ========== 日志配置 ==========
import logging.handlers
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE = os.getenv("LOG_FILE", "")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "")

# 飞书通知处理器
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
            title = f"{level_emoji} ES 同步告警 - {record.levelname}"
            
            content = f"**时间**: {self.format(record).split(' [')[0]}\n"
            content += f"**级别**: {record.levelname}\n"
            content += f"**消息**: {record.getMessage()}\n"
            
            if record.exc_info:
                import traceback
                content += f"**异常**: {traceback.format_exception(*record.exc_info)[:500]}\n"
            
            payload = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {
                            "tag": "plain_text",
                            "content": title
                        },
                        "template": "red" if record.levelno >= logging.ERROR else "orange"
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "tag": "lark_md",
                                "content": content
                            }
                        }
                    ]
                }
            }
            
            # 发送到飞书
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            
            if response.status_code != 200:
                print(f"飞书通知发送失败: {response.status_code} {response.text}", file=sys.stderr)
        
        except Exception as e:
            print(f"飞书通知处理器异常: {e}", file=sys.stderr)

# 设置日志处理器
handlers = []

# 标准输出处理器
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
handlers.append(console_handler)

# 文件处理器（如果指定了日志文件）
if LOG_FILE:
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    handlers.append(file_handler)

# 飞书通知处理器（如果配置了 Webhook）
if FEISHU_WEBHOOK:
    feishu_handler = FeishuHandler(FEISHU_WEBHOOK)
    feishu_handler.setLevel(logging.WARNING)  # 只处理 WARNING 及以上级别
    feishu_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    handlers.append(feishu_handler)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=LOG_FORMAT,
    datefmt=DATE_FORMAT,
    handlers=handlers
)
logger = logging.getLogger("es_to_bytehouse")

# ========== 配置部分（支持环境变量） ==========
# Elasticsearch 配置
ES_HOST = os.getenv("ES_HOST", "")
ES_USER = os.getenv("ES_USER", "")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")

# ByteHouse 配置
BYTEHOUSE_HOST = os.getenv("BYTEHOUSE_HOST", "")
BYTEHOUSE_PORT = int(os.getenv("BYTEHOUSE_PORT", "19000"))
BYTEHOUSE_USER = os.getenv("BYTEHOUSE_USER", "")
BYTEHOUSE_PASSWORD = os.getenv("BYTEHOUSE_PASSWORD", "")
BYTEHOUSE_SECURE = os.getenv("BYTEHOUSE_SECURE", "true").lower() == "true"

# 迁移配置
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
SCROLL_SIZE = int(os.getenv("SCROLL_SIZE", "1000"))
SCROLL_TIMEOUT = os.getenv("SCROLL_TIMEOUT", "5m")
TARGET_DATABASE = os.getenv("TARGET_DATABASE", "es_migration")

# 增量同步配置
SYNC_STATE_TABLE = "_sync_state"
INCREMENTAL_INTERVAL = int(os.getenv("INCREMENTAL_INTERVAL", "60"))
TIME_FIELD_CANDIDATES = ["timestamp", "@timestamp", "cTime", "StartTime", "ExecutionTime", "doc_time", "created_at", "updated_at", "_timestamp"]

# 索引过滤配置
INDEX_PATTERN = os.getenv("INDEX_PATTERN", "*")  # 要同步的索引模式

# 是否存储原始 _source JSON（默认不存储，节省空间）
STORE_SOURCE = os.getenv("STORE_SOURCE", "false").lower() == "true"

# ========== ES 类型到 ByteHouse 类型映射 ==========
# 全部使用 String 类型，避免类型转换问题
ES_TO_BYTEHOUSE_TYPE_MAP = {
    "keyword": "String",
    "text": "String",
    "match_only_text": "String",
    "long": "String",
    "integer": "String",
    "short": "String",
    "byte": "String",
    "double": "String",
    "float": "String",
    "half_float": "String",
    "scaled_float": "String",
    "boolean": "String",
    "date": "String",
    "ip": "String",
    "geo_point": "String",
    "geo_shape": "String",
    "binary": "String",
    "nested": "String",
    "object": "String",
    "flattened": "String",
    "constant_keyword": "String",
}


class ESToByteHouseMigrator:
    """ES 到 ByteHouse 数据迁移器"""
    
    def __init__(self):
        self.es_session = requests.Session()
        self.es_session.auth = (ES_USER, ES_PASSWORD)
        self.bytehouse_client = None
        self.current_table_columns = set()  # 当前表的列名集合
        
    def connect_bytehouse(self) -> bool:
        """连接 ByteHouse"""
        try:
            logger.info(f"正在连接 ByteHouse: {BYTEHOUSE_HOST}:{BYTEHOUSE_PORT}")
            logger.debug(f"  用户: {BYTEHOUSE_USER}, SSL: {BYTEHOUSE_SECURE}")
            
            self.bytehouse_client = Client(
                host=BYTEHOUSE_HOST,
                port=BYTEHOUSE_PORT,
                user=BYTEHOUSE_USER,
                password=BYTEHOUSE_PASSWORD,
                secure=BYTEHOUSE_SECURE,
                verify=False,  # 跳过 SSL 证书验证
            )
            # 测试连接
            result = self.bytehouse_client.execute("SELECT 1")
            logger.info(f"✓ ByteHouse 连接成功")
            return True
        except Exception as e:
            logger.error(f"✗ ByteHouse 连接失败: {e}")
            return False
    
    def test_es_connection(self) -> bool:
        """测试 ES 连接"""
        try:
            logger.info(f"正在连接 Elasticsearch: {ES_HOST}")
            response = self.es_session.get(f"{ES_HOST}/_cluster/health")
            response.raise_for_status()
            health = response.json()
            logger.info(f"✓ ES 连接成功")
            logger.info(f"  集群名称: {health.get('cluster_name')}")
            logger.info(f"  集群状态: {health.get('status')}")
            logger.info(f"  节点数量: {health.get('number_of_nodes')}")
            logger.info(f"  数据节点: {health.get('number_of_data_nodes')}")
            return True
        except Exception as e:
            logger.error(f"✗ ES 连接失败: {e}")
            return False
    
    def get_es_indices(self, pattern: str = "*") -> List[str]:
        """获取 ES 索引列表"""
        try:
            logger.debug(f"获取索引列表，模式: {pattern}")
            response = self.es_session.get(
                f"{ES_HOST}/_cat/indices/{pattern}",
                params={"format": "json", "h": "index,docs.count,store.size"}
            )
            response.raise_for_status()
            indices = response.json()
            # 过滤掉系统索引
            user_indices = [
                idx for idx in indices 
                if not idx['index'].startswith('.')
            ]
            logger.debug(f"找到 {len(user_indices)} 个用户索引")
            return user_indices
        except Exception as e:
            logger.error(f"获取索引列表失败: {e}")
            return []
    
    def get_es_mapping(self, index: str) -> Dict[str, Any]:
        """获取 ES 索引的 mapping"""
        try:
            logger.debug(f"获取索引 {index} 的 mapping")
            response = self.es_session.get(f"{ES_HOST}/{index}/_mapping")
            response.raise_for_status()
            mapping = response.json()
            # 返回第一个匹配索引的 mapping
            for idx_name, idx_mapping in mapping.items():
                return idx_mapping.get('mappings', {}).get('properties', {})
            return {}
        except Exception as e:
            logger.warning(f"获取 mapping 失败: {e}")
            return {}
    
    def flatten_mapping(self, mapping: Dict, prefix: str = "") -> Dict[str, str]:
        """
        将嵌套的 ES mapping 展平为字段名 -> 类型的映射
        例如: {"agent": {"properties": {"id": {"type": "keyword"}}}}
        转换为: {"agent_id": "keyword"}
        """
        result = {}
        for field_name, field_config in mapping.items():
            full_name = f"{prefix}_{field_name}" if prefix else field_name
            # 清理字段名，替换不合法字符
            full_name = full_name.replace(".", "_").replace("-", "_").replace("@", "")
            
            if "properties" in field_config:
                # 嵌套对象，递归展平
                nested = self.flatten_mapping(field_config["properties"], full_name)
                result.update(nested)
            elif "type" in field_config:
                es_type = field_config["type"]
                result[full_name] = es_type
        return result
    
    def es_type_to_bytehouse(self, es_type: str) -> str:
        """将 ES 类型转换为 ByteHouse 类型"""
        return ES_TO_BYTEHOUSE_TYPE_MAP.get(es_type, "String")
    
    def generate_create_table_sql(self, table_name: str, mapping: Dict[str, str]) -> str:
        """生成 CREATE TABLE SQL - 包含全量展平字段"""
        columns = []
        
        # 添加原始 _id 字段
        columns.append("    `_id` String")
        
        # 记录表的列名
        self.current_table_columns = {"_id", "_timestamp"}
        
        # 是否存储原始文档 JSON
        if STORE_SOURCE:
            columns.append("    `_source` String")
            self.current_table_columns.add("_source")
        
        # 添加时间戳字段
        columns.append("    `_timestamp` DateTime64(3) DEFAULT now64(3)")
        
        # 根据 mapping 添加展平的字段
        for field_name, es_type in mapping.items():
            if field_name in self.current_table_columns:
                continue
            bh_type = self.es_type_to_bytehouse(es_type)
            # 所有字段都设为 Nullable，因为 ES 数据可能缺失字段
            columns.append(f"    `{field_name}` Nullable({bh_type})")
            self.current_table_columns.add(field_name)
        
        columns_str = ",\n".join(columns)
        
        # 使用 CnchMergeTree 引擎（ByteHouse 专用），添加 UNIQUE KEY 支持 upsert
        sql = f"""CREATE TABLE IF NOT EXISTS `{TARGET_DATABASE}`.`{table_name}` (
{columns_str}
) ENGINE = CnchMergeTree()
ORDER BY tuple()
UNIQUE KEY _id"""
        
        return sql
    
    def scan_all_fields(self, index: str, sample_size: int = 5000) -> Dict[str, str]:
        """扫描实际数据，收集所有出现过的字段（展平后）"""
        all_fields = {}
        scanned = 0
        start_time = time.time()
        
        logger.info(f"  开始扫描数据收集全量字段...")
        logger.info(f"    样本数量: {sample_size}")
        
        try:
            response = self.es_session.post(
                f"{ES_HOST}/{index}/_search",
                params={"scroll": "2m"},
                json={"size": 1000, "query": {"match_all": {}}}
            )
            response.raise_for_status()
            data = response.json()
            
            scroll_id = data.get("_scroll_id")
            hits = data.get("hits", {}).get("hits", [])
            
            while hits and scanned < sample_size:
                for hit in hits:
                    source = hit.get("_source", {})
                    flat_doc = self.flatten_document(source)
                    for field_name, value in flat_doc.items():
                        if field_name not in all_fields:
                            # 推断类型
                            all_fields[field_name] = self.infer_type(value)
                    scanned += 1
                    if scanned >= sample_size:
                        break
                
                if scanned >= sample_size:
                    break
                
                if scanned % 1000 == 0:
                    logger.debug(f"    已扫描 {scanned} 条，发现 {len(all_fields)} 个字段")
                    
                response = self.es_session.post(
                    f"{ES_HOST}/_search/scroll",
                    json={"scroll": "2m", "scroll_id": scroll_id}
                )
                response.raise_for_status()
                data = response.json()
                scroll_id = data.get("_scroll_id")
                hits = data.get("hits", {}).get("hits", [])
            
            # 清理 scroll
            if scroll_id:
                self.es_session.delete(f"{ES_HOST}/_search/scroll", json={"scroll_id": scroll_id})
                
        except Exception as e:
            logger.error(f"  扫描字段时出错: {e}")
        
        elapsed = time.time() - start_time
        logger.info(f"  扫描完成: {scanned} 条数据，{len(all_fields)} 个字段，耗时 {elapsed:.1f}s")
        return all_fields
    
    def infer_type(self, value) -> str:
        """根据值推断 ES 类型 - 统一返回 keyword（存为 String）"""
        return "keyword"
    
    def create_database(self) -> bool:
        """创建目标数据库"""
        try:
            logger.info(f"创建数据库: {TARGET_DATABASE}")
            self.bytehouse_client.execute(f"CREATE DATABASE IF NOT EXISTS `{TARGET_DATABASE}`")
            logger.info(f"✓ 数据库 {TARGET_DATABASE} 就绪")
            return True
        except Exception as e:
            logger.error(f"✗ 创建数据库失败: {e}")
            return False
    
    def create_sync_state_table(self) -> bool:
        """创建同步状态表"""
        try:
            logger.debug(f"创建同步状态表: {SYNC_STATE_TABLE}")
            sql = f"""CREATE TABLE IF NOT EXISTS `{TARGET_DATABASE}`.`{SYNC_STATE_TABLE}` (
    `index_name` String,
    `table_name` String,
    `time_field` String,
    `update_time_field` String DEFAULT '',
    `last_sync_time` String,
    `last_update_time` String DEFAULT '',
    `last_sync_count` Int64,
    `updated_at` DateTime64(3) DEFAULT now64(3)
) ENGINE = CnchMergeTree()
ORDER BY (index_name)"""
            self.bytehouse_client.execute(sql)
            logger.debug(f"✓ 同步状态表就绪")
            return True
        except Exception as e:
            logger.error(f"✗ 创建同步状态表失败: {e}")
            return False
    
    def get_sync_state(self, index_name: str) -> Optional[Dict]:
        """获取索引的同步状态"""
        try:
            result = self.bytehouse_client.execute(
                f"SELECT index_name, table_name, time_field, last_sync_time, update_time_field, last_update_time FROM `{TARGET_DATABASE}`.`{SYNC_STATE_TABLE}` WHERE index_name = %(index)s ORDER BY updated_at DESC LIMIT 1",
                {"index": index_name}
            )
            if result:
                return {
                    "index_name": result[0][0],
                    "table_name": result[0][1],
                    "time_field": result[0][2],
                    "last_sync_time": result[0][3],
                    "update_time_field": result[0][4] if len(result[0]) > 4 else "",
                    "last_update_time": result[0][5] if len(result[0]) > 5 else ""
                }
            return None
        except Exception as e:
            return None
    
    def update_sync_state(self, index_name: str, table_name: str, time_field: str, last_sync_time: str, count: int, update_time_field: str = "", last_update_time: str = ""):
        """更新同步状态"""
        try:
            self.bytehouse_client.execute(
                f"INSERT INTO `{TARGET_DATABASE}`.`{SYNC_STATE_TABLE}` (index_name, table_name, time_field, last_sync_time, last_sync_count, update_time_field, last_update_time) VALUES",
                [(index_name, table_name, time_field, last_sync_time, count, update_time_field, last_update_time)]
            )
            logger.debug(f"  同步状态已更新: {index_name} -> {last_sync_time}")
            if update_time_field and last_update_time:
                logger.debug(f"    更新时间字段: {update_time_field} -> {last_update_time}")
        except Exception as e:
            logger.warning(f"  更新同步状态失败: {e}")
    
    def detect_time_field(self, index: str) -> Optional[str]:
        """检测 ES 索引的时间字段"""
        mapping = self.get_es_mapping(index)
        flat_mapping = self.flatten_mapping(mapping) if mapping else {}
        
        # 按优先级检查候选字段
        for candidate in TIME_FIELD_CANDIDATES:
            clean_candidate = candidate.replace("@", "")
            if clean_candidate in flat_mapping:
                return clean_candidate
        
        # 如果没有找到，尝试从数据中检测
        try:
            response = self.es_session.post(
                f"{ES_HOST}/{index}/_search",
                json={"size": 1, "query": {"match_all": {}}}
            )
            response.raise_for_status()
            hits = response.json().get("hits", {}).get("hits", [])
            if hits:
                source = hits[0].get("_source", {})
                flat_doc = self.flatten_document(source)
                for candidate in TIME_FIELD_CANDIDATES:
                    clean_candidate = candidate.replace("@", "")
                    if clean_candidate in flat_doc:
                        return clean_candidate
        except:
            pass
        
        return None
    
    def detect_update_time_field(self, index: str) -> Optional[str]:
        """检测 ES 索引的更新时间字段（用于支持 UPDATE 操作）"""
        update_field_candidates = ["updated_at", "updateTime", "update_time", "modifiedAt", "modified_at", "lastModified"]
        
        mapping = self.get_es_mapping(index)
        flat_mapping = self.flatten_mapping(mapping) if mapping else {}
        
        # 检查候选字段
        for candidate in update_field_candidates:
            clean_candidate = candidate.replace("@", "")
            if clean_candidate in flat_mapping:
                return clean_candidate
        
        # 从数据中检测
        try:
            response = self.es_session.post(
                f"{ES_HOST}/{index}/_search",
                json={"size": 1, "query": {"match_all": {}}}
            )
            response.raise_for_status()
            hits = response.json().get("hits", {}).get("hits", [])
            if hits:
                source = hits[0].get("_source", {})
                flat_doc = self.flatten_document(source)
                for candidate in update_field_candidates:
                    clean_candidate = candidate.replace("@", "")
                    if clean_candidate in flat_doc:
                        return clean_candidate
        except:
            pass
        
        return None
    
    def create_table(self, table_name: str, mapping: Dict[str, str]) -> bool:
        """在 ByteHouse 创建表"""
        try:
            sql = self.generate_create_table_sql(table_name, mapping)
            logger.debug(f"创建表 SQL:\n{sql}")
            self.bytehouse_client.execute(sql)
            logger.info(f"✓ 表 {TARGET_DATABASE}.{table_name} 创建成功")
            logger.info(f"    字段数量: {len(mapping) + 3}")  # +3 for _id, _source, _timestamp
            return True
        except Exception as e:
            logger.error(f"✗ 创建表失败: {e}")
            return False
    
    def flatten_document(self, doc: Dict, prefix: str = "") -> Dict[str, Any]:
        """将嵌套文档展平，所有值转为字符串"""
        result = {}
        for key, value in doc.items():
            full_key = f"{prefix}_{key}" if prefix else key
            full_key = full_key.replace(".", "_").replace("-", "_").replace("@", "")
            
            if isinstance(value, dict):
                # 嵌套对象，递归展平
                nested = self.flatten_document(value, full_key)
                result.update(nested)
            elif isinstance(value, list):
                # 列表处理：检查元素类型
                if len(value) > 0 and isinstance(value[0], dict):
                    # 如果列表元素是对象，展开为 fieldname_0_key, fieldname_1_key
                    for idx, item in enumerate(value):
                        if isinstance(item, dict):
                            item_nested = self.flatten_document(item, f"{full_key}_{idx}")
                            result.update(item_nested)
                        else:
                            # 如果列表中有非对象元素，转为 JSON
                            result[full_key] = json.dumps(value, ensure_ascii=False)
                            break
                else:
                    # 列表元素是简单类型，转为 JSON 字符串
                    result[full_key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, str):
                # 字符串类型：尝试解析为 JSON
                if value and (value.startswith('{') or value.startswith('[')):
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, dict):
                            # JSON 对象，展开
                            nested = self.flatten_document(parsed, full_key)
                            result.update(nested)
                        elif isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], dict):
                            # JSON 数组且元素是对象，展开
                            for idx, item in enumerate(parsed):
                                if isinstance(item, dict):
                                    item_nested = self.flatten_document(item, f"{full_key}_{idx}")
                                    result.update(item_nested)
                        else:
                            # 其他 JSON 类型，保持原始字符串
                            result[full_key] = value
                    except (json.JSONDecodeError, ValueError):
                        # 不是有效 JSON，保持原始字符串
                        result[full_key] = value
                else:
                    result[full_key] = value
            elif value is None:
                result[full_key] = None
            else:
                # 所有其他值转为字符串
                result[full_key] = str(value)
        return result
    
    def scroll_es_data(self, index: str) -> int:
        """使用 scroll API 从 ES 获取数据并插入到 ByteHouse"""
        table_name = self.sanitize_table_name(index)
        total_migrated = 0
        start_time = time.time()
        last_log_time = start_time
        
        try:
            # 初始化 scroll
            response = self.es_session.post(
                f"{ES_HOST}/{index}/_search",
                params={"scroll": SCROLL_TIMEOUT},
                json={"size": SCROLL_SIZE, "query": {"match_all": {}}}
            )
            response.raise_for_status()
            data = response.json()
            
            scroll_id = data.get("_scroll_id")
            hits = data.get("hits", {}).get("hits", [])
            total_docs = data.get("hits", {}).get("total", {})
            if isinstance(total_docs, dict):
                total_docs = total_docs.get("value", 0)
            
            logger.info(f"开始迁移数据...")
            logger.info(f"    索引: {index}")
            logger.info(f"    目标表: {TARGET_DATABASE}.{table_name}")
            logger.info(f"    文档总数: {total_docs:,}")
            logger.info(f"    批次大小: {SCROLL_SIZE}")
            
            while hits:
                # 批量插入数据
                batch = []
                for hit in hits:
                    doc_id = hit.get("_id", "")
                    source = hit.get("_source", {})
                    
                    # 构建行数据
                    row = {"_id": doc_id}
                    
                    # 是否存储原始 JSON
                    if STORE_SOURCE:
                        row["_source"] = json.dumps(source, ensure_ascii=False)
                    
                    # 展平文档字段
                    flat_doc = self.flatten_document(source)
                    row.update(flat_doc)
                    batch.append(row)
                
                if batch:
                    # 全量同步时也检测并添加新字段
                    new_fields = self.detect_new_fields_from_batch(batch)
                    if new_fields:
                        added = self.add_new_columns(table_name, new_fields)
                        if added > 0:
                            logger.info(f"    发现并添加 {added} 个新字段: {', '.join(list(new_fields.keys())[:5])}{'...' if len(new_fields) > 5 else ''}")
                    
                    self.insert_batch(table_name, batch)
                    total_migrated += len(batch)
                    
                    # 每 10 秒或每 10000 条打印一次进度
                    current_time = time.time()
                    if current_time - last_log_time >= 10 or total_migrated % 10000 == 0:
                        elapsed = current_time - start_time
                        speed = total_migrated / elapsed if elapsed > 0 else 0
                        progress = (total_migrated / total_docs * 100) if total_docs > 0 else 0
                        eta = (total_docs - total_migrated) / speed if speed > 0 else 0
                        logger.info(f"    进度: {total_migrated:,}/{total_docs:,} ({progress:.1f}%) | 速度: {speed:.0f}/s | 预计剩余: {eta:.0f}s")
                        last_log_time = current_time
                
                # 获取下一批数据
                response = self.es_session.post(
                    f"{ES_HOST}/_search/scroll",
                    json={"scroll": SCROLL_TIMEOUT, "scroll_id": scroll_id}
                )
                response.raise_for_status()
                data = response.json()
                scroll_id = data.get("_scroll_id")
                hits = data.get("hits", {}).get("hits", [])
            
            # 清理 scroll 上下文
            if scroll_id:
                self.es_session.delete(
                    f"{ES_HOST}/_search/scroll",
                    json={"scroll_id": scroll_id}
                )
            
            elapsed = time.time() - start_time
            speed = total_migrated / elapsed if elapsed > 0 else 0
            logger.info(f"✓ 索引 {index} 迁移完成")
            logger.info(f"    总计: {total_migrated:,} 条")
            logger.info(f"    耗时: {elapsed:.1f}s")
            logger.info(f"    平均速度: {speed:.0f} 条/秒")
            return total_migrated
            
        except Exception as e:
            logger.error(f"✗ 迁移索引 {index} 失败: {e}")
            return total_migrated
    
    def insert_batch(self, table_name: str, batch: List[Dict]):
        """批量插入数据到 ByteHouse - 只插入表中定义的列"""
        if not batch:
            return
        
        # 只使用表中定义的列
        columns = sorted(list(self.current_table_columns - {"_timestamp"}))  # _timestamp 有默认值
        
        # 构建插入数据
        rows = []
        for row in batch:
            row_data = tuple(row.get(col) for col in columns)
            rows.append(row_data)
        
        # 构建 INSERT 语句
        columns_str = ", ".join(f"`{col}`" for col in columns)
        sql = f"INSERT INTO `{TARGET_DATABASE}`.`{table_name}` ({columns_str}) VALUES"
        
        try:
            self.bytehouse_client.execute(sql, rows, types_check=True)
        except Exception as e:
            error_msg = str(e)
            # 如果是列不存在错误，说明有字段没有被添加
            if "No such column" in error_msg or "SQLSTATE: 42703" in error_msg:
                logger.error(f"    插入失败：表中缺少列：{error_msg}")
                # 重新加载表结构
                self.load_table_columns(table_name)
                # 检测并添加缺少的列
                new_fields = self.detect_new_fields_from_batch(batch)
                if new_fields:
                    added = self.add_new_columns(table_name, new_fields)
                    logger.info(f"    重试前添加了 {added} 个缺少的列")
                    # 重新构建 columns 和 rows
                    columns = sorted(list(self.current_table_columns - {"_timestamp"}))
                    rows = []
                    for row in batch:
                        row_data = tuple(row.get(col) for col in columns)
                        rows.append(row_data)
                    columns_str = ", ".join(f"`{col}`" for col in columns)
                    sql = f"INSERT INTO `{TARGET_DATABASE}`.`{table_name}` ({columns_str}) VALUES"
                    # 重试插入
                    try:
                        self.bytehouse_client.execute(sql, rows, types_check=True)
                        logger.info(f"    重试插入成功")
                        return
                    except Exception as e2:
                        logger.error(f"    重试插入仍然失败: {e2}")
                else:
                    logger.error(f"    未检测到新字段，无法修复")
            else:
                logger.warning(f"    批量插入失败，尝试逐条插入: {e}")
            
            # 尝试逐条插入（用于其他类型错误）
            success = 0
            for i, row in enumerate(rows):
                try:
                    self.bytehouse_client.execute(sql, [row], types_check=True)
                    success += 1
                except Exception as e2:
                    if i < 3:  # 只打印前几条错误
                        logger.debug(f"    第 {i} 条数据插入失败: {e2}")
            logger.info(f"    逐条插入完成: 成功 {success}/{len(rows)}")
    
    def sanitize_table_name(self, index_name: str) -> str:
        """清理索引名，使其成为合法的表名"""
        # 替换不合法字符
        table_name = index_name.replace("-", "_").replace(".", "_")
        # 如果以数字开头，添加前缀
        if table_name[0].isdigit():
            table_name = f"idx_{table_name}"
        return table_name
    
    def migrate_index(self, index: str) -> bool:
        """迁移单个索引"""
        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"开始迁移索引: {index}")
        logger.info(f"{'='*60}")
        
        # 获取 mapping 中的字段
        mapping = self.get_es_mapping(index)
        flat_mapping = self.flatten_mapping(mapping) if mapping else {}
        logger.info(f"  Mapping 中字段数量: {len(flat_mapping)}")
        
        # 扫描实际数据获取全量字段
        scanned_fields = self.scan_all_fields(index)
        
        # 合并 mapping 和扫描得到的字段（扫描结果优先补充）
        all_fields = flat_mapping.copy()
        new_fields_count = 0
        for field_name, field_type in scanned_fields.items():
            if field_name not in all_fields:
                all_fields[field_name] = field_type
                new_fields_count += 1
        
        logger.info(f"  从数据中补充字段: {new_fields_count} 个")
        logger.info(f"  合并后全量字段数量: {len(all_fields)}")
        
        # 创建表
        table_name = self.sanitize_table_name(index)
        if not self.create_table(table_name, all_fields):
            return False
        
        # 迁移数据
        migrated = self.scroll_es_data(index)
        return migrated > 0
    
    def run(self, index_pattern: str = "*", exclude_system: bool = True):
        """运行迁移"""
        logger.info("="*60)
        logger.info("ES to ByteHouse 数据迁移工具")
        logger.info("="*60)
        
        # 测试连接
        if not self.test_es_connection():
            return
        
        if not self.connect_bytehouse():
            return
        
        # 创建目标数据库
        if not self.create_database():
            return
        
        # 获取索引列表
        indices = self.get_es_indices(index_pattern)
        if not indices:
            logger.warning("没有找到要迁移的索引")
            return
        
        logger.info(f"")
        logger.info(f"找到 {len(indices)} 个索引:")
        for idx in indices:
            logger.info(f"  - {idx['index']} (文档数: {idx.get('docs.count', 'N/A')}, 大小: {idx.get('store.size', 'N/A')})")
        
        # 迁移每个索引
        success_count = 0
        for idx_info in indices:
            index_name = idx_info['index']
            try:
                if self.migrate_index(index_name):
                    success_count += 1
            except Exception as e:
                logger.error(f"✗ 迁移索引 {index_name} 时发生错误: {e}")
        
        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"迁移完成: 成功 {success_count}/{len(indices)} 个索引")
        logger.info(f"{'='*60}")
    
    def scroll_es_incremental(self, index: str, time_field: str, since_time: str, update_time_field: str = "", since_update_time: str = "") -> tuple:
        """增量查询 ES 数据并插入到 ByteHouse（支持新增和更新）
        
        返回: (total_migrated, max_time, max_update_time)
        """
        table_name = self.sanitize_table_name(index)
        total_migrated = 0
        max_time = since_time
        max_update_time = since_update_time
        start_time = time.time()
        
        # 构建时间范围查询，使用原始字段名（可能带@）
        original_time_field = time_field
        if time_field == "timestamp" and "@timestamp" in str(self.get_es_mapping(index)):
            original_time_field = "@timestamp"
        
        # 构建查询条件
        # 如果有更新时间字段，使用 OR 查询同时获取新增和更新的数据
        if update_time_field and since_update_time:
            query = {
                "bool": {
                    "should": [
                        {"range": {original_time_field: {"gt": since_time}}},
                        {"range": {update_time_field: {"gt": since_update_time}}}
                    ],
                    "minimum_should_match": 1
                }
            }
            logger.info(f"  查询条件: {original_time_field} > {since_time} OR {update_time_field} > {since_update_time}")
        else:
            query = {
                "bool": {
                    "filter": [
                        {"range": {original_time_field: {"gt": since_time}}}
                    ]
                }
            }
        
        try:
            response = self.es_session.post(
                f"{ES_HOST}/{index}/_search",
                params={"scroll": SCROLL_TIMEOUT},
                json={
                    "size": SCROLL_SIZE,
                    "query": query,
                    "sort": [{original_time_field: "asc"}]
                }
            )
            response.raise_for_status()
            data = response.json()
            
            scroll_id = data.get("_scroll_id")
            hits = data.get("hits", {}).get("hits", [])
            total_docs = data.get("hits", {}).get("total", {})
            if isinstance(total_docs, dict):
                total_docs = total_docs.get("value", 0)
            
            if total_docs == 0:
                logger.info(f"  无增量数据（上次同步: {since_time}）")
                return (0, max_time, max_update_time)
            
            if update_time_field:
                logger.info(f"  发现 {total_docs:,} 条增量/更新数据")
            else:
                logger.info(f"  发现 {total_docs:,} 条增量数据")
            logger.debug(f"    时间字段: {original_time_field}")
            logger.debug(f"    起始时间: {since_time}")
            
            total_new_columns = 0
            while hits:
                batch = []
                for hit in hits:
                    doc_id = hit.get("_id", "")
                    source = hit.get("_source", {})
                    
                    row = {"_id": doc_id}
                    
                    # 是否存储原始 JSON
                    if STORE_SOURCE:
                        row["_source"] = json.dumps(source, ensure_ascii=False)
                    
                    flat_doc = self.flatten_document(source)
                    row.update(flat_doc)
                    batch.append(row)
                    
                    # 更新最大时间
                    doc_time = flat_doc.get(time_field, "")
                    if doc_time and doc_time > max_time:
                        max_time = doc_time
                    
                    # 更新最大更新时间
                    if update_time_field:
                        doc_update_time = flat_doc.get(update_time_field, "")
                        if doc_update_time and doc_update_time > max_update_time:
                            max_update_time = doc_update_time
                
                if batch:
                    # 检测并添加新字段
                    new_fields = self.detect_new_fields_from_batch(batch)
                    if new_fields:
                        added = self.add_new_columns(table_name, new_fields)
                        if added > 0:
                            total_new_columns += added
                            logger.info(f"  发现并添加 {added} 个新字段")
                    
                    self.insert_batch(table_name, batch)
                    total_migrated += len(batch)
                    if total_migrated % 5000 == 0 or total_migrated == total_docs:
                        logger.info(f"  已同步 {total_migrated:,}/{total_docs:,} 条")
                
                response = self.es_session.post(
                    f"{ES_HOST}/_search/scroll",
                    json={"scroll": SCROLL_TIMEOUT, "scroll_id": scroll_id}
                )
                response.raise_for_status()
                data = response.json()
                scroll_id = data.get("_scroll_id")
                hits = data.get("hits", {}).get("hits", [])
            
            if scroll_id:
                self.es_session.delete(f"{ES_HOST}/_search/scroll", json={"scroll_id": scroll_id})
            
            # 打印完成信息
            if total_migrated > 0:
                elapsed = time.time() - start_time
                logger.info(f"  ✓ 增量同步完成: {total_migrated:,} 条，耗时 {elapsed:.1f}s")
                if total_new_columns > 0:
                    logger.info(f"    新增字段: {total_new_columns} 个")
                logger.debug(f"    最新同步时间: {max_time}")
                if update_time_field and max_update_time:
                    logger.debug(f"    最新更新时间: {max_update_time}")
            
            return (total_migrated, max_time, max_update_time)
            
        except Exception as e:
            logger.error(f"  增量同步失败: {e}")
            return (total_migrated, max_time, max_update_time)
    
    def sync_index_incremental(self, index: str) -> int:
        """对单个索引执行增量同步"""
        table_name = self.sanitize_table_name(index)
        
        # 获取同步状态
        state = self.get_sync_state(index)
        if not state:
            logger.warning(f"  索引 {index} 未进行过全量同步，请先执行全量同步")
            return 0
        
        time_field = state["time_field"]
        last_sync_time = state["last_sync_time"]
        update_time_field = state.get("update_time_field", "")
        last_update_time = state.get("last_update_time", "")
        
        if not time_field or not last_sync_time:
            logger.warning(f"  索引 {index} 缺少时间字段信息，无法增量同步")
            return 0
        
        # 加载表的列信息
        self.load_table_columns(table_name)
        
        logger.debug(f"  时间字段: {time_field}")
        logger.debug(f"  上次同步: {last_sync_time}")
        if update_time_field:
            logger.debug(f"  更新时间字段: {update_time_field}")
            logger.debug(f"  上次更新同步: {last_update_time}")
        
        # 执行增量同步
        total_migrated, max_time, max_update_time = self.scroll_es_incremental(
            index, time_field, last_sync_time, update_time_field, last_update_time
        )
        
        # 更新同步状态
        if total_migrated > 0:
            # 确保 update_time_field 和 max_update_time 不为 None
            safe_update_time_field = update_time_field or ""
            safe_max_update_time = max_update_time or ""
            self.update_sync_state(
                index, table_name, time_field, max_time, total_migrated,
                safe_update_time_field, safe_max_update_time
            )
        
        return total_migrated
    
    def load_table_columns(self, table_name: str):
        """加载已有表的列信息"""
        try:
            result = self.bytehouse_client.execute(
                f"DESCRIBE TABLE `{TARGET_DATABASE}`.`{table_name}`"
            )
            self.current_table_columns = {row[0] for row in result}
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
            
            bh_type = self.es_type_to_bytehouse(field_type)
            try:
                sql = f"ALTER TABLE `{TARGET_DATABASE}`.`{table_name}` ADD COLUMN `{field_name}` Nullable({bh_type})"
                self.bytehouse_client.execute(sql)
                self.current_table_columns.add(field_name)
                added_count += 1
                logger.debug(f"    添加新列: {field_name}")
            except Exception as e:
                # 列可能已存在，忽略错误
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
    
    def run_incremental(self, index_pattern: str = "*", continuous: bool = False, interval: int = INCREMENTAL_INTERVAL):
        """运行增量同步"""
        logger.info("="*60)
        logger.info("ES to ByteHouse 增量同步")
        logger.info("="*60)
        logger.info(f"模式: {'持续同步' if continuous else '单次同步'}")
        if continuous:
            logger.info(f"同步间隔: {interval} 秒")
        logger.info(f"索引模式: {index_pattern}")
        
        if not self.test_es_connection():
            return
        
        if not self.connect_bytehouse():
            return
        
        round_count = 0
        while True:
            round_count += 1
            indices = self.get_es_indices(index_pattern)
            if not indices:
                logger.warning("没有找到要同步的索引")
                if not continuous:
                    return
                time.sleep(interval)
                continue
            
            logger.info(f"")
            logger.info(f"[第 {round_count} 轮] 开始增量同步 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            logger.info(f"待检查索引数: {len(indices)}")
            
            total_synced = 0
            for idx_info in indices:
                index_name = idx_info['index']
                logger.info(f"")
                logger.info(f"检查索引: {index_name}")
                try:
                    synced = self.sync_index_incremental(index_name)
                    total_synced += synced
                except Exception as e:
                    logger.error(f"  ✗ 同步失败: {e}")
            
            logger.info(f"")
            logger.info(f"[第 {round_count} 轮] 同步完成，本轮共同步 {total_synced:,} 条")
            
            if not continuous:
                break
            
            logger.info(f"等待 {interval} 秒后进行下一轮...")
            time.sleep(interval)
    
    def migrate_index_with_state(self, index: str) -> bool:
        """迁移索引并记录同步状态（用于后续增量同步）"""
        result = self.migrate_index(index)
        
        if result:
            # 检测时间字段
            time_field = self.detect_time_field(index)
            update_time_field = self.detect_update_time_field(index)
            table_name = self.sanitize_table_name(index)
            
            if time_field:
                # 获取最大时间值
                try:
                    max_time_result = self.bytehouse_client.execute(
                        f"SELECT max(`{time_field}`) FROM `{TARGET_DATABASE}`.`{table_name}` WHERE `{time_field}` IS NOT NULL AND `{time_field}` != ''"
                    )
                    max_time = max_time_result[0][0] if max_time_result and max_time_result[0][0] else ""
                    
                    # 获取最大更新时间（如果有更新时间字段）
                    max_update_time = ""
                    if update_time_field:
                        try:
                            max_update_result = self.bytehouse_client.execute(
                                f"SELECT max(`{update_time_field}`) FROM `{TARGET_DATABASE}`.`{table_name}` WHERE `{update_time_field}` IS NOT NULL AND `{update_time_field}` != ''"
                            )
                            max_update_time = max_update_result[0][0] if max_update_result and max_update_result[0][0] else ""
                        except:
                            pass
                    
                    if max_time:
                        # 确保 max_update_time 不为 None
                        final_max_update_time = str(max_update_time) if max_update_time else ""
                        self.update_sync_state(index, table_name, time_field, str(max_time), 0, update_time_field or "", final_max_update_time)
                        logger.info(f"  ✓ 已记录同步状态")
                        logger.info(f"    时间字段: {time_field}")
                        logger.info(f"    最大时间: {max_time}")
                        if update_time_field and final_max_update_time:
                            logger.info(f"    更新时间字段: {update_time_field}")
                            logger.info(f"    最大更新时间: {final_max_update_time}")
                            logger.info(f"  ✓ 支持 UPDATE 同步（通过 {update_time_field} 字段）")
                    else:
                        logger.warning(f"  ⚠ 时间字段 {time_field} 无有效数据")
                except Exception as e:
                    logger.warning(f"  ⚠ 记录同步状态失败: {e}")
            else:
                logger.warning(f"  ⚠ 未检测到时间字段，无法进行增量同步")
        
        return result
    
    def run_full_sync(self, index_pattern: str = "*"):
        """运行全量同步（带状态记录）"""
        logger.info("="*60)
        logger.info("ES to ByteHouse 全量同步")
        logger.info("="*60)
        logger.info(f"索引模式: {index_pattern}")
        logger.info(f"目标数据库: {TARGET_DATABASE}")
        logger.info(f"批次大小: {SCROLL_SIZE}")
        
        if not self.test_es_connection():
            return
        
        if not self.connect_bytehouse():
            return
        
        if not self.create_database():
            return
        
        # 创建同步状态表
        self.create_sync_state_table()
        
        indices = self.get_es_indices(index_pattern)
        if not indices:
            logger.warning("没有找到要迁移的索引")
            return
        
        logger.info(f"")
        logger.info(f"找到 {len(indices)} 个索引:")
        total_docs = 0
        for idx in indices:
            doc_count = idx.get('docs.count', '0')
            try:
                total_docs += int(doc_count)
            except:
                pass
            logger.info(f"  - {idx['index']} (文档数: {doc_count}, 大小: {idx.get('store.size', 'N/A')})")
        logger.info(f"总文档数: {total_docs:,}")
        
        start_time = time.time()
        success_count = 0
        for idx_info in indices:
            index_name = idx_info['index']
            try:
                if self.migrate_index_with_state(index_name):
                    success_count += 1
            except Exception as e:
                logger.error(f"✗ 迁移索引 {index_name} 时发生错误: {e}")
        
        elapsed = time.time() - start_time
        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"全量同步完成")
        logger.info(f"  成功索引: {success_count}/{len(indices)}")
        logger.info(f"  总耗时: {elapsed:.1f}s ({elapsed/60:.1f}m)")
        logger.info(f"{'='*60}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="ES to ByteHouse 数据迁移工具（支持全量和增量同步）")
    parser.add_argument(
        "--pattern", "-p",
        default="*",
        help="索引模式，支持通配符，例如 'logs-*'，默认为 '*'"
    )
    parser.add_argument(
        "--list-only", "-l",
        action="store_true",
        help="仅列出索引，不执行迁移"
    )
    parser.add_argument(
        "--index", "-i",
        help="指定要迁移的单个索引名"
    )
    parser.add_argument(
        "--mode", "-m",
        choices=["full", "incremental", "continuous"],
        default="full",
        help="同步模式: full=全量同步, incremental=单次增量同步, continuous=持续增量同步"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=INCREMENTAL_INTERVAL,
        help=f"持续增量同步的间隔秒数，默认 {INCREMENTAL_INTERVAL} 秒"
    )
    
    args = parser.parse_args()
    
    migrator = ESToByteHouseMigrator()
    
    if args.list_only:
        if not migrator.test_es_connection():
            return
        indices = migrator.get_es_indices(args.pattern)
        logger.info(f"")
        logger.info(f"索引列表 (模式: {args.pattern}):")
        for idx in indices:
            logger.info(f"  - {idx['index']} (文档数: {idx.get('docs.count', 'N/A')}, 大小: {idx.get('store.size', 'N/A')})")
    elif args.mode == "full":
        # 全量同步
        pattern = args.index if args.index else args.pattern
        migrator.run_full_sync(index_pattern=pattern)
    elif args.mode == "incremental":
        # 单次增量同步
        pattern = args.index if args.index else args.pattern
        migrator.run_incremental(index_pattern=pattern, continuous=False)
    elif args.mode == "continuous":
        # 持续增量同步
        pattern = args.index if args.index else args.pattern
        migrator.run_incremental(index_pattern=pattern, continuous=True, interval=args.interval)


if __name__ == "__main__":
    main()
