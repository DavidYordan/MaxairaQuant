from __future__ import annotations
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from ..config.schema import ClickHouseConfig

def get_client(cfg: ClickHouseConfig) -> Client:
    # 先确保数据库存在（用 default 连接执行创建）
    bootstrap = clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database="default",
        send_receive_timeout=30,  # 30秒超时
    )
    bootstrap.command(f"CREATE DATABASE IF NOT EXISTS {cfg.database}")
    bootstrap.close()  # 及时关闭临时连接
    
    client = clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
        send_receive_timeout=30,
        # 高性能连接池配置 - 硬编码优化参数
        pool_mgr=True,
        pool_size=16,           # 适中的连接池大小
        max_pool_size=32,       # 最大32个连接
        pool_reset=False,       # 避免频繁重置
        # HTTP连接优化
        compress=True,          # 启用压缩
        query_limit=0,          # 无查询限制
    )
    
    # 高性能异步插入配置 - 硬编码优化
    client.command("SET async_insert = 1")
    client.command("SET wait_for_async_insert = 1")
    client.command("SET async_insert_max_data_size = 33554432")      # 32MB
    client.command("SET async_insert_busy_timeout_ms = 1000")        # 1秒刷新
    client.command("SET async_insert_stale_timeout_ms = 2000")       # 2秒过期
    
    # ClickHouse性能优化设置
    client.command("SET max_threads = 8")                            # 8线程
    client.command("SET max_memory_usage = 4294967296")             # 4GB内存
    client.command("SET max_insert_block_size = 8388608")           # 8MB块
    client.command("SET min_insert_block_size_rows = 100000")       # 10万行
    client.command("SET min_insert_block_size_bytes = 67108864")    # 64MB
    
    # 网络和IO优化
    client.command("SET network_compression_method = 'lz4'")         # LZ4压缩
    client.command("SET max_concurrent_queries_for_user = 16")       # 16并发查询
        
    return client