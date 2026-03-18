#!/usr/bin/env python3
"""
ClickHouse to ByteHouse 数据同步工具
支持全量同步和增量同步
"""

import os
import sys
import time
import json
import logging
import requests
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import clickhouse_driver
from clickhouse_driver import Client

# 加载 .env 文件（如果存在）
try:
    from dotenv import load_dotenv
    # 尝试加载两个可能的配置文件
    load_dotenv('clickhouse_to_bytehouse.env')
    load_dotenv('.env')  # 作为备选
except ImportError:
    pass  # 如果没有安装 python-dotenv，忽略


# 配置日志
import sys
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "")

# 设置日志级别
numeric_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(level=numeric_level, format='%(asctime)s [%(levelname)s] %(message)s')

logger = logging.getLogger(__name__)

# 添加文件处理器（如果指定了日志文件且文件路径存在）
if LOG_FILE:
    try:
        # 检查目录是否存在，如果不存在则不创建文件处理器
        import os
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir and os.path.exists(log_dir):
            file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
            file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        else:
            # 如果目录不存在，只使用控制台输出
            logger.warning(f"日志目录不存在: {log_dir}，仅使用控制台输出")
    except Exception as e:
        logger.warning(f"无法创建日志文件处理器: {e}，仅使用控制台输出")

# 添加标准输出处理器
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# 源 ClickHouse 配置
SOURCE_HOST = os.getenv("SOURCE_CH_HOST", "")
SOURCE_PORT = int(os.getenv("SOURCE_CH_PORT", "9000"))
SOURCE_USER = os.getenv("SOURCE_CH_USER", "default")
SOURCE_PASSWORD = os.getenv("SOURCE_CH_PASSWORD", "")
SOURCE_DATABASE = os.getenv("SOURCE_CH_DATABASE", "default")

# 目标 ByteHouse 配置
TARGET_HOST = os.getenv("TARGET_BH_HOST", "")
TARGET_PORT = int(os.getenv("TARGET_BH_PORT", "19000"))
TARGET_USER = os.getenv("TARGET_BH_USER", "bytehouse")
TARGET_PASSWORD = os.getenv("TARGET_BH_PASSWORD", "")
TARGET_DATABASE = os.getenv("TARGET_BH_DATABASE", "default")

# 同步配置
BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "1000"))
INCREMENTAL_INTERVAL = int(os.getenv("INCREMENTAL_INTERVAL", "60"))  # 秒
TABLE_PATTERN = os.getenv("TABLE_PATTERN", "*")  # 支持逗号分隔多个模式


def match_tables(tables: list, pattern: str) -> list:
    """匹配表名，支持逗号分隔的多个模式"""
    import fnmatch
    if pattern == "*":
        return tables
    
    # 分割多个模式（逗号分隔）
    patterns = [p.strip() for p in pattern.split(",") if p.strip()]
    
    matched = set()
    for p in patterns:
        for t in tables:
            if fnmatch.fnmatch(t, p) or t == p:
                matched.add(t)
    
    return list(matched)


class ClickHouseToByteHouseSync:
    def __init__(self):
        self.source_client = None
        self.target_client = None
        self.current_table_columns = set()

    def connect_source(self):
        """连接源 ClickHouse"""
        try:
            self.source_client = Client(
                host=SOURCE_HOST,
                port=SOURCE_PORT,
                user=SOURCE_USER,
                password=SOURCE_PASSWORD,
                database=SOURCE_DATABASE,
                secure=False
            )
            # 测试连接
            self.source_client.execute("SELECT 1")
            logger.info(f"✓ 源 ClickHouse 连接成功: {SOURCE_HOST}:{SOURCE_PORT}")
            return True
        except Exception as e:
            logger.error(f"✗ 源 ClickHouse 连接失败: {e}")
            return False

    def connect_target(self):
        """连接目标 ByteHouse"""
        try:
            self.target_client = Client(
                host=TARGET_HOST,
                port=TARGET_PORT,
                user=TARGET_USER,
                password=TARGET_PASSWORD,
                # 不指定数据库，先连接到默认数据库
                secure=True
            )
            # 测试连接
            self.target_client.execute("SELECT 1")
            
            # 检查并创建目标数据库
            try:
                self.target_client.execute(f"CREATE DATABASE IF NOT EXISTS `{TARGET_DATABASE}`")
                logger.info(f"✓ 目标数据库 {TARGET_DATABASE} 准备就绪")
            except Exception as e:
                logger.warning(f"创建目标数据库失败（可能权限不足）: {e}")
            
            # 切换到目标数据库
            self.target_client.execute(f"USE `{TARGET_DATABASE}`")
            
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
CREATE TABLE IF NOT EXISTS `{TARGET_DATABASE}`.`_sync_state` (
    `table_name` String,
    `last_sync_time` String,
    `last_update_time` String DEFAULT '',
    `sync_count` UInt64,
    `sync_time` DateTime DEFAULT now()
) ENGINE = CnchMergeTree()
ORDER BY (`table_name`, `sync_time`)
SETTINGS index_granularity = 8192
"""
            self.target_client.execute(sql)
            logger.info("✓ 同步状态表 _sync_state 创建成功")
        except Exception as e:
            logger.error(f"✗ 创建同步状态表失败: {e}")

    def get_last_sync_time(self, table_name: str) -> Tuple[str, str]:
        """获取表的最后同步时间（时间字段值和更新时间字段值）"""
        try:
            result = self.target_client.execute(
                f"SELECT max(last_sync_time), max(last_update_time) FROM `_sync_state` WHERE `table_name` = '{table_name}'"
            )
            if result and result[0]:
                last_time = result[0][0] if result[0][0] else ""
                last_update_time = result[0][1] if result[0][1] else ""
                return last_time, last_update_time
        except Exception as e:
            logger.warning(f"获取同步状态失败: {e}")
        
        return "", ""

    def update_sync_state(self, table_name: str, last_time: str, last_update_time: str = "", sync_count: int = 0):
        """更新同步状态"""
        try:
            # 获取最大的时间值作为最新的同步时间
            self.target_client.execute(
                f"INSERT INTO `_sync_state` (`table_name`, `last_sync_time`, `last_update_time`, `sync_count`) VALUES",
                [(table_name, last_time, last_update_time, sync_count)]
            )
            logger.debug(f"同步状态已更新: {table_name}, 时间={last_time}")
        except Exception as e:
            logger.error(f"更新同步状态失败: {e}")

    def query_sync_state(self, table_name: str = ""):
        """查询同步状态"""
        try:
            if table_name:
                result = self.target_client.execute(
                    f"SELECT `table_name`, `last_sync_time`, `last_update_time`, `sync_count`, `sync_time` FROM `_sync_state` WHERE `table_name` = '{table_name}' ORDER BY `sync_time` DESC LIMIT 10"
                )
            else:
                result = self.target_client.execute(
                    f"SELECT `table_name`, `last_sync_time`, `last_update_time`, `sync_count`, `sync_time` FROM `_sync_state` ORDER BY `sync_time` DESC LIMIT 100"
                )
            
            logger.info(f"同步状态查询结果 (共 {len(result)} 条):")
            for row in result:
                logger.info(f"  表: {row[0]}, 最后同步时间: {row[1]}, 更新时间: {row[2]}, 同步数量: {row[3]}, 状态时间: {row[4]}")
                
            return result
        except Exception as e:
            logger.error(f"查询同步状态失败: {e}")
            return []

    def get_source_tables(self, db_name: str = "") -> List[str]:
        """获取源数据库中的表列表"""
        if not db_name:
            db_name = SOURCE_DATABASE
        
        # 使用反引号包围数据库名，防止包含特殊字符导致语法错误
        tables = self.source_client.execute(f"SHOW TABLES FROM `{db_name}`")
        return [table[0] for table in tables]

    def get_table_schema(self, client, table_name: str, db_name: str) -> List[Tuple[str, str]]:
        """获取表结构"""
        schema = client.execute(f"DESCRIBE TABLE `{db_name}`.`{table_name}`")
        return [(col[0], col[1]) for col in schema]  # (column_name, type)

    def create_target_table(self, table_name: str, schema: List[Tuple[str, str]]) -> bool:
        """在目标 ByteHouse 创建表"""
        try:
            # 构建列定义
            columns = []
            for col_name, col_type in schema:
                # 确保列名安全
                safe_col_name = f"`{col_name}`"
                columns.append(f"{safe_col_name} {col_type}")
            
            columns_str = ",\n".join(columns)
            sql = f"""
CREATE TABLE IF NOT EXISTS `{TARGET_DATABASE}`.`{table_name}` (
{columns_str}
) ENGINE = CnchMergeTree()
ORDER BY tuple()
"""
            self.target_client.execute(sql)
            logger.info(f"✓ 表 {TARGET_DATABASE}.{table_name} 创建成功")
            return True
        except Exception as e:
            logger.error(f"✗ 创建表 {table_name} 失败: {e}")
            return False

    def load_target_table_columns(self, table_name: str):
        """加载目标表的列信息"""
        try:
            columns = self.target_client.execute(f"DESCRIBE TABLE `{TARGET_DATABASE}`.`{table_name}`")
            self.current_table_columns = {col[0] for col in columns}
            logger.debug(f"  表 {table_name} 有 {len(self.current_table_columns)} 个列")
        except Exception as e:
            logger.warning(f"  加载表结构失败: {e}")
            self.current_table_columns = set()

    def sync_table_full(self, table_name: str) -> bool:
        """全量同步单个表"""
        logger.info(f"开始全量同步表: {table_name}")
        
        # 获取源表结构
        source_schema = self.get_table_schema(self.source_client, table_name, SOURCE_DATABASE)
        logger.info(f"  源表结构: {len(source_schema)} 列")
        
        # 在目标创建表
        if not self.create_target_table(table_name, source_schema):
            return False
        
        # 加载目标表列信息
        self.load_target_table_columns(table_name)
        
        # 获取源表数据总量
        count_result = self.source_client.execute(f"SELECT COUNT(*) FROM `{SOURCE_DATABASE}`.`{table_name}`")
        total_rows = count_result[0][0]
        logger.info(f"  源表数据量: {total_rows:,} 行")
        
        if total_rows == 0:
            logger.info("  源表为空，跳过同步")
            return True
        
        # 分批同步数据
        offset = 0
        total_synced = 0
        start_time = time.time()
        
        while offset < total_rows:
            # 查询批次数据
            batch_sql = f"SELECT * FROM `{SOURCE_DATABASE}`.`{table_name}` LIMIT {BATCH_SIZE} OFFSET {offset}"
            batch_data = self.source_client.execute(batch_sql)
            
            if not batch_data:
                break
            
            # 插入到目标表
            columns_str = ", ".join([f"`{col[0]}`" for col in source_schema])
            insert_sql = f"INSERT INTO `{TARGET_DATABASE}`.`{table_name}` ({columns_str}) VALUES"
            
            try:
                self.target_client.execute(insert_sql, batch_data, types_check=True)
                batch_size = len(batch_data)
                total_synced += batch_size
                offset += BATCH_SIZE
                
                # 计算进度
                progress = min(100, total_synced / total_rows * 100)
                elapsed = time.time() - start_time
                speed = total_synced / elapsed if elapsed > 0 else 0
                eta = (total_rows - total_synced) / speed if speed > 0 else 0
                
                logger.info(f"  进度: {total_synced:,}/{total_rows:,} ({progress:.1f}%) | 速度: {speed:.0f}/s | 预计剩余: {eta:.0f}s")
                
            except Exception as e:
                logger.error(f"  批次插入失败: {e}")
                return False
        
        elapsed = time.time() - start_time
        logger.info(f"✓ 表 {table_name} 全量同步完成")
        logger.info(f"    同步行数: {total_synced:,}")
        logger.info(f"    耗时: {elapsed:.1f}s")
        logger.info(f"    平均速度: {total_synced/elapsed:.0f}/s" if elapsed > 0 else "")
        
        return True

    def sync_table_incremental(self, table_name: str, since_condition: str = "", time_column: str = "created_at") -> int:
        """增量同步单个表，返回同步的行数"""
        logger.info(f"开始增量同步表: {table_name}")
        
        # 构建查询条件
        where_clause = f"WHERE {since_condition}" if since_condition else ""
        sql = f"SELECT * FROM `{SOURCE_DATABASE}`.`{table_name}` {where_clause}"
        
        # 查询数据
        try:
            data = self.source_client.execute(sql)
            
            if not data:
                logger.info(f"  无新增数据")
                return 0
            
            # 获取表结构用于构建 INSERT 语句
            schema = self.get_table_schema(self.source_client, table_name, SOURCE_DATABASE)
            columns_str = ", ".join([f"`{col[0]}`" for col in schema])
            insert_sql = f"INSERT INTO `{TARGET_DATABASE}`.`{table_name}` ({columns_str}) VALUES"
            
            # 插入数据
            self.target_client.execute(insert_sql, data, types_check=True)
            
            synced_count = len(data)
            logger.info(f"  增量同步完成: {synced_count} 行")
            return synced_count
            
        except Exception as e:
            logger.error(f"  增量同步失败: {e}")
            return 0

    def run_full_sync(self, table_pattern: str = "*"):
        """运行全量同步"""
        logger.info("="*60)
        logger.info("ClickHouse to ByteHouse 全量同步")
        logger.info("="*60)
        logger.info(f"源数据库: {SOURCE_DATABASE}")
        logger.info(f"目标数据库: {TARGET_DATABASE}")
        logger.info(f"表模式: {table_pattern}")
        logger.info(f"批次大小: {BATCH_SIZE}")
        
        if not self.connect_source():
            return
        
        if not self.connect_target():
            return
        
        # 获取源表列表
        tables = self.get_source_tables()
        if table_pattern != "*":
            tables = match_tables(tables, table_pattern)
        
        if not tables:
            logger.warning("没有找到要同步的表")
            return
        
        logger.info(f"")
        logger.info(f"找到 {len(tables)} 个表:")
        for table in tables:
            logger.info(f"  - {table}")
        
        # 同步每个表
        success_count = 0
        for table_name in tables:
            try:
                if self.sync_table_full(table_name):
                    success_count += 1
            except Exception as e:
                logger.error(f"✗ 同步表 {table_name} 时发生错误: {e}")
        
        logger.info(f"")
        logger.info(f"{'='*60}")
        logger.info(f"全量同步完成: 成功 {success_count}/{len(tables)} 个表")
        logger.info(f"{'='*60}")

    def run_incremental(self, table_pattern: str = "*", time_column: str = "created_at", 
                       continuous: bool = False, interval: int = INCREMENTAL_INTERVAL, start_date: str = ""):
        """运行增量同步"""
        logger.info("="*60)
        logger.info("ClickHouse to ByteHouse 增量同步")
        logger.info("="*60)
        logger.info(f"模式: {'持续同步' if continuous else '单次同步'}")
        if continuous:
            logger.info(f"同步间隔: {interval} 秒")
        logger.info(f"表模式: {table_pattern}")
        logger.info(f"时间字段: {time_column}")
        if start_date:
            logger.info(f"开始日期: {start_date}")
        
        if not self.connect_source():
            return
        
        if not self.connect_target():
            return
        
        # 获取源表列表
        tables = self.get_source_tables()
        if table_pattern != "*":
            tables = match_tables(tables, table_pattern)
        
        if not tables:
            logger.warning("没有找到要同步的表")
            return
        
        round_count = 0
        while True:
            round_count += 1
            logger.info(f"")
            logger.info(f"[第 {round_count} 轮] 开始增量同步 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            
            total_synced = 0
            for table_name in tables:
                # 获取上次同步的时间点
                last_sync_time, _ = self.get_last_sync_time(table_name)
                
                if last_sync_time and not start_date:
                    # 如果有上次同步时间且未指定开始日期，则从上次时间点继续
                    since_condition = f"`{time_column}` > '{last_sync_time}'"
                    logger.info(f"  表 {table_name}: 增量同步条件 {since_condition}")
                elif start_date:
                    # 如果指定了开始日期，使用开始日期
                    since_condition = f"`{time_column}` > '{start_date}'"
                    logger.info(f"  表 {table_name}: 从指定日期开始同步，条件 {since_condition}")
                else:
                    # 首次同步，使用近段时间的数据
                    since_condition = f"`{time_column}` > now() - INTERVAL 1 DAY"
                    logger.info(f"  表 {table_name}: 首次增量同步，使用条件 {since_condition}")
                
                synced = self.sync_table_incremental(table_name, since_condition, time_column)
                
                # 更新同步状态
                if synced > 0:
                    # 获取当前批次的最大时间
                    try:
                        max_time_result = self.source_client.execute(
                            f"SELECT max(`{time_column}`) FROM `{SOURCE_DATABASE}`.`{table_name}` WHERE {since_condition}"
                        )
                        if max_time_result and max_time_result[0][0]:
                            max_time = str(max_time_result[0][0])
                            self.update_sync_state(table_name, max_time, "", synced)
                    except Exception as e:
                        logger.warning(f"  更新同步状态失败: {e}")
                
                total_synced += synced
            
            logger.info(f"")
            logger.info(f"[第 {round_count} 轮] 同步完成，本轮共同步 {total_synced:,} 行")
            
            if not continuous:
                break
            
            logger.info(f"等待 {interval} 秒后进行下一轮...")
            time.sleep(interval)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="ClickHouse to ByteHouse 数据同步工具")
    parser.add_argument("--mode", choices=["full", "incremental", "status"], default="full", 
                       help="同步模式: full(全量), incremental(增量) 或 status(查看同步状态)")
    parser.add_argument("--table-pattern", default=TABLE_PATTERN,
                       help="表名模式，支持逗号分隔多个，如 'table1,table2,log_*'，默认从 TABLE_PATTERN 环境变量读取")
    parser.add_argument("--time-column", default="created_at", 
                       help="增量同步使用的时间字段")
    parser.add_argument("--start-date", default="", 
                       help="开始日期，格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--continuous", action="store_true", 
                       help="持续增量同步模式")
    parser.add_argument("--interval", type=int, default=INCREMENTAL_INTERVAL,
                       help="增量同步间隔（秒）")
    
    args = parser.parse_args()
    
    sync = ClickHouseToByteHouseSync()
    
    if args.mode == "full":
        sync.run_full_sync(args.table_pattern)
    elif args.mode == "incremental":
        sync.run_incremental(
            table_pattern=args.table_pattern,
            time_column=args.time_column,
            continuous=args.continuous,
            interval=args.interval,
            start_date=args.start_date
        )
    elif args.mode == "status":
        # 连接目标数据库并查询同步状态
        if sync.connect_target():
            sync.query_sync_state(args.table_pattern if args.table_pattern != "*" else "")

if __name__ == "__main__":
    main()