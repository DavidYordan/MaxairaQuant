from __future__ import annotations
import asyncio
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from ..config.schema import ClickHouseConfig
from loguru import logger
from dataclasses import dataclass


class AsyncClickHouseClient:
    """
    官方 AsyncClient 封装版本
    提供异步 query / command / insert 接口，并保留简单并发控制
    """

    def __init__(self, async_client: AsyncClient, pool_size: int = 8):
        self._client = async_client
        self._semaphore = asyncio.Semaphore(pool_size)

    async def query(self, query: str, parameters=None):
        """异步查询"""
        async with self._semaphore:
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

    async def close(self):
        """关闭连接"""
        await self._client.close()


@dataclass
class ClickHouseClients:
    read: AsyncClickHouseClient
    write: AsyncClickHouseClient
    backfill: AsyncClickHouseClient

async def get_clients(
    cfg: ClickHouseConfig,
    pool_read: int = 8,
    pool_write: int = 8,
    pool_backfill: int = 16,
) -> ClickHouseClients:
    """
    创建三个 ClickHouse 客户端：read / write / backfill，对应不同并发池。
    """
    try:
        # 步骤1：使用异步方式、临时客户端来确保数据库存在
        # 这避免了在 async 函数中使用同步 IO，并隔离了引导操作
        bootstrap_client_raw = None
        try:
            bootstrap_client_raw = await clickhouse_connect.get_async_client(
                host=cfg.host,
                port=cfg.port,
                username=cfg.username,
                password=cfg.password,
                database="default",
                connect_timeout=10,
                send_receive_timeout=30
            )
            await bootstrap_client_raw.command(f"CREATE DATABASE IF NOT EXISTS {cfg.database}")
            logger.info(f"✅ 数据库 '{cfg.database}' 已确认存在")
        finally:
            if bootstrap_client_raw:
                await bootstrap_client_raw.close()
        
        # 稍微延迟，给予数据库准备时间，避免后续连接立即失败
        await asyncio.sleep(0.1)

        def make_sync():
            return clickhouse_connect.get_client(
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

        # 分别创建三个独立的底层同步 client
        sync_read = make_sync()
        sync_write = make_sync()
        sync_backfill = make_sync()

        # 包装为 AsyncClient，再包为 AsyncClickHouseClient，并设置各自的并发池大小
        read_client = AsyncClickHouseClient(AsyncClient(client=sync_read), pool_size=pool_read)
        write_client = AsyncClickHouseClient(AsyncClient(client=sync_write), pool_size=pool_write)
        backfill_client = AsyncClickHouseClient(AsyncClient(client=sync_backfill), pool_size=pool_backfill)

        # 基础配置（对三个客户端都执行，确保一致性）
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
            await read_client.command(cmd)
            await write_client.command(cmd)
            await backfill_client.command(cmd)

        logger.info("✅ ClickHouse 读/写/回填 三客户端初始化完成")
        return ClickHouseClients(
            read=read_client,
            write=write_client,
            backfill=backfill_client,
        )
    except Exception as e:
        logger.error(f"❌ ClickHouse 多客户端初始化失败: {e}")
        raise
