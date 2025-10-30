from __future__ import annotations
import asyncio
import logging
from typing import List, Optional
from ..common.types import Kline
from ..db.queries import insert_klines
from ..services.ws.event_bus import EventBus


class DataBuffer:
    def __init__(self, client, table_name: str, batch_size: int = 2000, flush_interval_ms: int = 1500, 
                 event_bus: Optional[EventBus] = None, writer_workers: int = 4, max_queue_size: int = 20000):
        self.client = client
        self.table_name = table_name
        self.batch_size = batch_size
        self.flush_interval_ms = flush_interval_ms
        self.writer_workers = writer_workers  # 多写入器支持
        self.max_queue_size = max_queue_size
        
        self._buf: List[Kline] = []
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        
        # 高性能写入队列配置
        self._write_q: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._writer_tasks: List[asyncio.Task] = []  # 多个写入任务
        self.event_bus = event_bus
        
        # 性能监控
        self._stats = {
            'total_writes': 0,
            'failed_writes': 0,
            'queue_full_drops': 0,
            'avg_batch_size': 0
        }
        self.logger = logging.getLogger(f"DataBuffer.{table_name}")

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._flush_loop())
        
        # 启动多个写入工作器
        if not self._writer_tasks:
            for i in range(self.writer_workers):
                task = asyncio.create_task(self._writer_loop(worker_id=i))
                self._writer_tasks.append(task)
                
        self.logger.info(f"Started DataBuffer with {self.writer_workers} writers, queue size {self.max_queue_size}")

    async def stop(self):
        if self._task:
            self._task.cancel()
            self._task = None
        await self.flush()
        
        # 等待写队列排空并停止所有写入任务
        while not self._write_q.empty():
            await asyncio.sleep(0.1)
            
        for task in self._writer_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._writer_tasks.clear()
        
        self.logger.info(f"Stopped DataBuffer. Stats: {self._stats}")

    async def add(self, k: Kline):
        async with self._lock:
            self._buf.append(k)
            if len(self._buf) >= self.batch_size:
                await self.flush()

    async def add_many(self, items: List[Kline]):
        """批量添加数据，提高效率"""
        async with self._lock:
            self._buf.extend(items)
            # 如果缓冲区过大，分批刷新
            while len(self._buf) >= self.batch_size:
                batch = self._buf[:self.batch_size]
                self._buf = self._buf[self.batch_size:]
                await self._queue_write(batch)

    async def flush(self):
        """刷新缓冲区到写入队列"""
        async with self._lock:
            if self._buf:
                batch = self._buf.copy()
                self._buf.clear()
                await self._queue_write(batch)

    async def _queue_write(self, batch: List[Kline]):
        """将批次加入写入队列，处理队列满的情况"""
        try:
            self._write_q.put_nowait(batch)
        except asyncio.QueueFull:
            # 队列满时的处理策略：丢弃最老的批次
            try:
                self._write_q.get_nowait()  # 移除最老的
                self._write_q.put_nowait(batch)  # 添加新的
                self._stats['queue_full_drops'] += 1
                self.logger.warning(f"Queue full, dropped oldest batch. Total drops: {self._stats['queue_full_drops']}")
            except asyncio.QueueEmpty:
                pass

    async def _flush_loop(self):
        """定时刷新循环"""
        while True:
            try:
                await asyncio.sleep(self.flush_interval_ms / 1000.0)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Flush loop error: {e}")

    async def _writer_loop(self, worker_id: int):
        """写入工作器循环"""
        self.logger.info(f"Writer worker {worker_id} started")
        while True:
            try:
                batch = await self._write_q.get()
                await self._write_with_retry(batch, worker_id)
                self._write_q.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Writer worker {worker_id} error: {e}")

    async def _write_with_retry(self, batch: List[Kline], worker_id: int, max_retries: int = 5):
        """带重试的写入逻辑，增强错误处理"""
        for attempt in range(max_retries):
            try:
                # 使用 asyncio.to_thread 避免阻塞事件循环
                await asyncio.to_thread(insert_klines, self.client, self.table_name, batch)
                
                # 更新统计信息
                self._stats['total_writes'] += 1
                self._stats['avg_batch_size'] = (
                    (self._stats['avg_batch_size'] * (self._stats['total_writes'] - 1) + len(batch)) 
                    / self._stats['total_writes']
                )
                
                # 发布事件通知
                if self.event_bus:
                    await self.event_bus.publish("klines_written", {
                        "table": self.table_name,
                        "count": len(batch),
                        "worker_id": worker_id
                    })
                return
                
            except Exception as e:
                self._stats['failed_writes'] += 1
                wait_time = min(2 ** attempt, 30)  # 指数退避，最大30秒
                self.logger.error(
                    f"Writer {worker_id} attempt {attempt + 1}/{max_retries} failed: {e}. "
                    f"Retrying in {wait_time}s..."
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"Writer {worker_id} failed to write batch after {max_retries} attempts")

    def status(self):
        """返回缓冲区状态信息"""
        return {
            "table": self.table_name,
            "buffer_size": len(self._buf),
            "batch_size": self.batch_size,
            "flush_interval_ms": self.flush_interval_ms,
            "running": self._task is not None,
            "writer_queue_size": self._write_q.qsize(),
            "max_queue_size": self.max_queue_size,
            "writer_workers": self.writer_workers,
            "stats": self._stats.copy()
        }