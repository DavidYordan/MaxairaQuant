from __future__ import annotations
import asyncio
import logging
from typing import List, Optional
from ..common.types import Kline
from ..db.queries import insert_klines
from ..services.ws.event_bus import EventBus
from ..db.client import AsyncClickHouseClient


class DataBuffer:
    def __init__(self, client: AsyncClickHouseClient, table_name: str, batch_size: int = 2000, flush_interval_ms: int = 1500, 
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
        self._stopped = False  # 添加停止标志
        
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
        """优雅停止数据缓冲区"""
        if self._stopped:
            return
            
        self.logger.info("正在停止 DataBuffer...")
        self._stopped = True
        
        # 1. 停止主任务
        if self._task:
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._task = None
        
        # 2. 刷新剩余数据
        await self.flush()
        
        # 3. 等待写队列排空（带超时）
        drain_timeout = 30.0  # 30秒超时
        start_time = asyncio.get_event_loop().time()
        
        while not self._write_q.empty():
            if asyncio.get_event_loop().time() - start_time > drain_timeout:
                self.logger.warning(f"写队列排空超时，剩余 {self._write_q.qsize()} 项")
                break
            await asyncio.sleep(0.1)
        
        # 4. 优雅停止所有写入器
        self.logger.info(f"停止 {len(self._writer_tasks)} 个写入器...")
        
        # 发送停止信号给所有写入器
        for _ in self._writer_tasks:
            try:
                await self._write_q.put(None)  # None 作为停止信号
            except asyncio.QueueFull:
                pass
        
        # 等待写入器完成
        if self._writer_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._writer_tasks, return_exceptions=True),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                self.logger.warning("部分写入器未能在超时时间内完成")
                
        self._writer_tasks.clear()
        
        self.logger.info(f"DataBuffer 已停止. 统计: {self._stats}")

    async def _writer_loop(self, worker_id: int):
        """写入器工作循环 - 改进的错误处理和优雅停止"""
        self.logger.info(f"Writer {worker_id} 已启动")
        
        while not self._stopped:
            try:
                # 等待批次数据或停止信号
                batch = await self._write_q.get()
                
                # 检查停止信号
                if batch is None:
                    self.logger.info(f"Writer {worker_id} 收到停止信号")
                    break
                
                # 处理批次
                await self._write_with_retry(batch, worker_id)
                
            except asyncio.CancelledError:
                self.logger.info(f"Writer {worker_id} 被取消")
                break
                
            except Exception as e:
                self.logger.error(f"Writer {worker_id} 意外错误: {e}")
                await asyncio.sleep(1.0)  # 防止错误循环
                
        self.logger.info(f"Writer {worker_id} 已停止")

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
        while not self._stopped:
            try:
                await asyncio.sleep(self.flush_interval_ms / 1000.0)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Flush loop error: {e}")

    async def _write_with_retry(self, batch: List[Kline], worker_id: int, max_retries: int = 5):
        """带重试的写入逻辑，增强错误处理和性能优化"""
        if not batch:
            return
            
        retry_delays = [0.5, 1.0, 2.0, 4.0, 8.0]  # 指数退避延迟
        
        for attempt in range(max_retries):
            try:
                # 使用异步客户端直接调用
                await insert_klines(self.client, self.table_name, batch)
                
                # 更新统计信息（原子操作）
                batch_size = len(batch)
                self._stats['total_writes'] += 1
                self._stats['avg_batch_size'] = (
                    (self._stats['avg_batch_size'] * (self._stats['total_writes'] - 1) + batch_size) 
                    / self._stats['total_writes']
                )
                
                # 异步发布事件通知
                if self.event_bus:
                    try:
                        await self.event_bus.publish("klines_written", {
                            "table": self.table_name,
                            "count": batch_size,
                            "worker_id": worker_id,
                            "timestamp": asyncio.get_event_loop().time()
                        })
                    except Exception as e:
                        self.logger.warning(f"事件发布失败: {e}")
                
                return  # 成功写入，退出重试循环
                
            except Exception as e:
                self._stats['failed_writes'] += 1
                
                # 检查是否为致命错误（不应重试）
                if self._is_fatal_error(e):
                    self.logger.error(f"Writer {worker_id} 遇到致命错误，不重试: {e}")
                    break
                
                # 计算重试延迟
                delay = retry_delays[min(attempt, len(retry_delays) - 1)]
                
                self.logger.error(
                    f"Writer {worker_id} 尝试 {attempt + 1}/{max_retries} 失败: {e}. "
                    f"将在 {delay}s 后重试..."
                )
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(
                        f"Writer {worker_id} 在 {max_retries} 次尝试后仍然失败，丢弃批次 "
                        f"(大小: {len(batch)})"
                    )
    
    def _is_fatal_error(self, error: Exception) -> bool:
        """判断是否为致命错误（不应重试）"""
        error_str = str(error).lower()
        fatal_patterns = [
            "table doesn't exist",
            "column doesn't exist", 
            "syntax error",
            "permission denied",
            "authentication failed"
        ]
        return any(pattern in error_str for pattern in fatal_patterns)

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