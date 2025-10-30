from __future__ import annotations
import asyncio
from typing import List, Optional
from ..common.types import Kline
from ..db.queries import insert_klines
from ..services.ws.event_bus import EventBus


class DataBuffer:
    def __init__(self, client, table_name: str, batch_size: int, flush_interval_ms: int, event_bus: Optional[EventBus] = None):
        self.client = client
        self.table_name = table_name
        self.batch_size = batch_size
        self.flush_interval_ms = flush_interval_ms
        self._buf: List[Kline] = []
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        # 后台写入队列与任务（解耦合阻塞 IO）
        self._write_q: asyncio.Queue = asyncio.Queue(maxsize=self.batch_size * 4)
        self._writer_task: Optional[asyncio.Task] = None
        self.event_bus = event_bus

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._flush_loop())
        if self._writer_task is None:
            self._writer_task = asyncio.create_task(self._writer_loop())

    async def stop(self):
        if self._task:
            self._task.cancel()
            self._task = None
        await self.flush()
        # 等待写队列尽可能排空再关闭写入任务
        if self._writer_task:
            while not self._write_q.empty():
                await asyncio.sleep(0.1)
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
            self._writer_task = None

    async def add(self, k: Kline):
        async with self._lock:
            self._buf.append(k)
            if len(self._buf) >= self.batch_size:
                await self.flush()

    async def add_many(self, items: List[Kline]):
        # 高效批量入缓冲；达到批量阈值即入写队列
        data = None
        async with self._lock:
            self._buf.extend(items)
            if len(self._buf) >= self.batch_size:
                data = self._buf
                self._buf = []
        if data:
            await self._write_q.put(data)

    async def flush(self):
        async with self._lock:
            if not self._buf:
                return
            data = self._buf
            self._buf = []
        # 改为入队，后台线程写入 ClickHouse
        await self._write_q.put(data)

    async def _flush_loop(self):
        try:
            while True:
                await asyncio.sleep(self.flush_interval_ms / 1000.0)
                await self.flush()
        except asyncio.CancelledError:
            pass

    async def _writer_loop(self):
        from loguru import logger
        try:
            while True:
                batch = await self._write_q.get()
                await self._write_with_retry(batch, logger)
        except asyncio.CancelledError:
            pass

    async def _write_with_retry(self, batch: List[Kline], logger, max_retries: int = 3):
        backoff = 0.5
        for attempt in range(1, max_retries + 1):
            try:
                # 将 ClickHouse 同步写入放到线程池，避免阻塞事件循环
                await asyncio.to_thread(insert_klines, self.client, self.table_name, batch)
                # 写入成功后发布事件（用于客户端推送）
                if self.event_bus:
                    await self.event_bus.publish(self.table_name, batch)
                return
            except Exception as e:
                logger.warning("写入重试 {}/{} 失败：{} {}", attempt, max_retries, self.table_name, str(e))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
        # 最终失败：保留批次以避免丢数，稍后重试
        await asyncio.sleep(1.0)
        await self._write_q.put(batch)

    def status(self):
        return {
            "table": self.table_name,
            "buffer_size": len(self._buf),
            "batch_size": self.batch_size,
            "flush_interval_ms": self.flush_interval_ms,
            "running": self._task is not None,
            "writer_queue_size": self._write_q.qsize(),
        }