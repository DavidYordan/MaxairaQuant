from __future__ import annotations
import asyncio
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from loguru import logger
from ..config.schema import ClickHouseConfig


class AsyncClickHouseClient:
    """对官方 AsyncClient 的串行封装：同一连接上绝不并发"""
    def __init__(self, async_client: AsyncClient):
        self._client = async_client
        self._lock = asyncio.Lock()

    async def query(self, query: str, parameters=None):
        async with self._lock:
            return await self._client.query(query=query, parameters=parameters)

    async def command(self, cmd: str, parameters=None):
        async with self._lock:
            return await self._client.command(cmd=cmd, parameters=parameters)

    async def insert(self, table: str, data, column_names=None):
        async with self._lock:
            return await self._client.insert(
                table=table,
                data=data,
                column_names=column_names,
            )

    async def close(self):
        await self._client.close()


class ClickHouseClientManager:
    """
    负责：
    - 启动时建必要的 read / write
    - bulkwrite 按需创建：get_bulkwrite() 时没有就建，有就复用
    - 限制 bulkwrite 最大数量
    """
    def __init__(self, cfg: ClickHouseConfig, max_bulkwrite: int = 4):
        self.cfg = cfg
        self.max_bulkwrite = max_bulkwrite

        self._read: AsyncClickHouseClient | None = None
        self._bulkread: AsyncClickHouseClient | None = None
        self._write: AsyncClickHouseClient | None = None
        self._bulkwrites: list[AsyncClickHouseClient] = []

        self._init_lock = asyncio.Lock()
        self._bulkwrite_lock = asyncio.Lock()

    async def init_base(self):
        """
        确保库存在 + 创建基础的 read / bulkread / write
        只会真正执行一次，其它协程进来会等这把锁
        """
        async with self._init_lock:
            if self._read is not None:
                return

            # 1) 确保库存在
            bootstrap = await clickhouse_connect.get_async_client(
                host=self.cfg.host,
                port=self.cfg.port,
                username=self.cfg.username,
                password=self.cfg.password,
                database="default",
                connect_timeout=10,
                send_receive_timeout=30,
            )
            try:
                await bootstrap.command(f"CREATE DATABASE IF NOT EXISTS {self.cfg.database}")
            finally:
                await bootstrap.close()

            await asyncio.sleep(0.05)

            def make_sync():
                return clickhouse_connect.get_client(
                    host=self.cfg.host,
                    port=self.cfg.port,
                    username=self.cfg.username,
                    password=self.cfg.password,
                    database=self.cfg.database,
                    send_receive_timeout=30,
                    connect_timeout=10,
                    compress=True,
                    query_limit=0,
                )

            self._read = AsyncClickHouseClient(AsyncClient(client=make_sync()))
            self._bulkread = AsyncClickHouseClient(AsyncClient(client=make_sync()))
            self._write = AsyncClickHouseClient(AsyncClient(client=make_sync()))

            # 基础配置
            base_cmds = [
                "SET max_threads = 8",
                "SET max_memory_usage = 4294967296",
            ]
            for cmd in base_cmds:
                await asyncio.gather(
                    self._read.command(cmd),
                    self._bulkread.command(cmd),
                    self._write.command(cmd),
                )

            logger.info("✅ ClickHouse base clients (read/bulkread/write) ready")

    async def _make_bulkwrite(self) -> AsyncClickHouseClient:
        """真正创建一条 bulkwrite 连接并做写配置"""
        def make_sync():
            return clickhouse_connect.get_client(
                host=self.cfg.host,
                port=self.cfg.port,
                username=self.cfg.username,
                password=self.cfg.password,
                database=self.cfg.database,
                send_receive_timeout=60,
                connect_timeout=10,
                compress=True,
            )

        cli = AsyncClickHouseClient(AsyncClient(client=make_sync()))
        # 写配置可以更激进
        write_cmds = [
            "SET async_insert = 1",
            "SET wait_for_async_insert = 0",   # 巨量写建议先不等
            "SET async_insert_max_data_size = 33554432",
            "SET async_insert_busy_timeout_ms = 1000",
            "SET async_insert_stale_timeout_ms = 2000",
            "SET max_threads = 8",
            "SET max_memory_usage = 8589934592",  # 8G
            "SET network_compression_method = 'lz4'",
        ]
        for cmd in write_cmds:
            await cli.command(cmd)
        return cli

    async def get_read(self) -> AsyncClickHouseClient:
        await self.init_base()
        return self._read

    async def get_bulkread(self) -> AsyncClickHouseClient:
        await self.init_base()
        return self._bulkread

    async def get_write(self) -> AsyncClickHouseClient:
        await self.init_base()
        return self._write

    async def get_bulkwrite(self) -> AsyncClickHouseClient:
        """
        如果已有 bulkwrite，就按轮询给一个；
        如果还没建够 max_bulkwrite，就新建一个；
        多协程同时进来靠 _bulkwrite_lock 保证只建一次。
        """
        await self.init_base()

        async with self._bulkwrite_lock:
            if self._bulkwrites:
                # 简单返回最后一个，或者也可以 round-robin
                cli = self._bulkwrites.pop(0)
                self._bulkwrites.append(cli)
                return cli

            # 第一次进来，一个都没有，就建一条
            cli = await self._make_bulkwrite()
            self._bulkwrites.append(cli)
            return cli
