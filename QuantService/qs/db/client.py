from __future__ import annotations
import asyncio
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from ..config.schema import ClickHouseConfig
from loguru import logger


class AsyncClickHouseClient:
    """
    官方 AsyncClient 封装版本
    提供异步 query / command / insert 接口，并保留简单并发控制
    """

    def __init__(self, async_client: AsyncClient, pool_size: int = 5):
        self._client = async_client
        self._semaphore = asyncio.Semaphore(pool_size)
        self._query_lock = asyncio.Lock()  # 添加查询锁防止并发冲突

    async def query(self, query: str, parameters=None):
        """异步查询 - 增强并发控制"""
        async with self._semaphore:
            # 对于简单查询使用锁来避免并发问题
            if self._is_simple_query(query):
                async with self._query_lock:
                    return await self._client.query(query=query, parameters=parameters)
            else:
                return await self._client.query(query=query, parameters=parameters)

    async def command(self, cmd: str, parameters=None):
        """异步命令"""
        async with self._semaphore:
            return await self._client.command(cmd=cmd, parameters=parameters)

    async def insert(self, table: str, data, column_names=None):
        """异步插入"""
        async with self._semaphore:
            return await self._client.insert(
                table=table,
                data=data,
                column_names=column_names,
            )

    def _is_simple_query(self, query: str) -> bool:
        """判断是否为简单查询（需要加锁保护）"""
        query_lower = query.lower().strip()
        # 对于配置查询和简单的SELECT使用锁
        simple_patterns = [
            'select host, port, username, password from proxy_config',
            'select symbol, market_type from trading_pair_config',
            'select count() from',
            'select min(',
            'select max(',
        ]
        return any(pattern in query_lower for pattern in simple_patterns)

    async def close(self):
        """关闭连接"""
        await self._client.close()


async def get_client(cfg: ClickHouseConfig) -> AsyncClickHouseClient:
    """
    初始化 ClickHouse 异步客户端
    使用官方 AsyncClient 封装
    """
    try:
        # 步骤1：确保数据库存在（使用 default 数据库临时连接）
        bootstrap = clickhouse_connect.get_client(
            host=cfg.host,
            port=cfg.port,
            username=cfg.username,
            password=cfg.password,
            database="default",
            send_receive_timeout=30,
            connect_timeout=10,
        )

        bootstrap.command(f"CREATE DATABASE IF NOT EXISTS {cfg.database}")
        bootstrap.close()

        # 步骤2：创建主同步 Client
        sync_client = clickhouse_connect.get_client(
            host=cfg.host,
            port=cfg.port,
            username=cfg.username,
            password=cfg.password,
            database=cfg.database,
            send_receive_timeout=30,
            connect_timeout=10,
            compress=True,
            query_limit=0,
        )

        # 步骤3：创建官方 AsyncClient
        async_client = AsyncClient(client=sync_client)

        # 步骤4：创建封装包装器
        client = AsyncClickHouseClient(async_client)

        # 步骤5：依次执行配置命令（不可并发，因为底层连接非线程安全）
        config_commands = [
            "SET async_insert = 1",
            "SET wait_for_async_insert = 1",
            "SET async_insert_max_data_size = 33554432",
            "SET async_insert_busy_timeout_ms = 1000",
            "SET async_insert_stale_timeout_ms = 2000",
            "SET max_threads = 8",
            "SET max_memory_usage = 4294967296",
            "SET max_insert_block_size = 8388608",
            "SET min_insert_block_size_rows = 100000",
            "SET min_insert_block_size_bytes = 67108864",
            "SET network_compression_method = 'lz4'",
            "SET max_concurrent_queries_for_user = 16",
        ]

        for cmd in config_commands:
            await client.command(cmd)

        logger.info("✅ ClickHouse 异步客户端初始化完成")
        return client

    except Exception as e:
        logger.error(f"❌ ClickHouse 客户端初始化失败: {e}")
        raise
