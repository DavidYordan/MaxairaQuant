from __future__ import annotations
import asyncio
import json
import time
from decimal import Decimal
import websockets
from loguru import logger

from ...common.types import Kline
from ...db.client import AsyncClickHouseClient
from ...gateways.binance_ws import build_subscription_url
from ...services.ws.event_bus import EventBus


class UpstreamStream:
    def __init__(self, base_ws_url: str, symbol: str, period: str, 
                 write_client: AsyncClickHouseClient,
                 table_name: str,
                 event_bus: EventBus,
                 heartbeat_timeout_ms: int, initial_backoff_ms: int, max_backoff_ms: int):
        self.base_ws_url = base_ws_url
        self.symbol = symbol
        self.period = period
        
        self.write_client = write_client
        self.table_name = table_name
        self.event_bus = event_bus
        self._kline_batch: list[Kline] = []
        self._batch_writer_task: asyncio.Task | None = None

        self.heartbeat_timeout_ms = heartbeat_timeout_ms
        self.initial_backoff_ms = initial_backoff_ms
        self.max_backoff_ms = max_backoff_ms
        self._task: asyncio.Task | None = None
        self._stop = False
        self._last_msg_ts: float = 0.0

    async def start(self):
        """启动WebSocket连接"""
        if self._task is not None:
            logger.warning(f"WS连接已在运行: {self.symbol} {self.period}")
            return
            
        self._stop = False
        self._task = asyncio.create_task(
            self._run_with_supervision(),
            name=f"ws_{self.symbol}_{self.period}"
        )
        self._batch_writer_task = asyncio.create_task(
            self._batch_writer_loop(),
            name=f"wswriter_{self.symbol}_{self.period}"
        )
        logger.info(f"WS连接任务已启动: {self.symbol} {self.period}")

    async def stop(self):
        """优雅停止WebSocket连接"""
        if self._task is None:
            return
            
        logger.info(f"正在停止WS连接: {self.symbol} {self.period}")
        self._stop = True
        
        # 取消主任务和写入任务
        if self._task:
            self._task.cancel()
        if self._batch_writer_task:
            self._batch_writer_task.cancel()
            
        tasks_to_wait = [t for t in [self._task, self._batch_writer_task] if t]
        
        try:
            await asyncio.wait_for(asyncio.gather(*tasks_to_wait, return_exceptions=True), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning(f"WS连接停止超时: {self.symbol} {self.period}")
        
        self._task = None
        self._batch_writer_task = None
        logger.info(f"WS连接已停止: {self.symbol} {self.period}")

    async def _run_with_supervision(self):
        """带监督的运行逻辑"""
        consecutive_failures = 0
        max_consecutive_failures = 10
        
        while not self._stop:
            try:
                await self._run_connection()
                consecutive_failures = 0  # 重置失败计数
                
            except asyncio.CancelledError:
                logger.info(f"WS连接被取消: {self.symbol} {self.period}")
                break
                
            except Exception as e:
                consecutive_failures += 1
                logger.error(
                    f"WS连接异常 {self.symbol} {self.period} "
                    f"(连续第{consecutive_failures}次): {e}"
                )
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(
                        f"WS连接连续失败过多，停止重连: {self.symbol} {self.period}"
                    )
                    break
                
                # 指数退避重连
                backoff_time = min(
                    self.initial_backoff_ms * (2 ** (consecutive_failures - 1)) / 1000.0,
                    self.max_backoff_ms / 1000.0
                )
                
                if not self._stop:
                    logger.info(
                        f"WS将在 {backoff_time:.1f}s 后重连: {self.symbol} {self.period}"
                    )
                    try:
                        await asyncio.wait_for(
                            asyncio.Event().wait(),  # 永远不会被设置的事件
                            timeout=backoff_time
                        )
                    except asyncio.TimeoutError:
                        pass  # 正常超时，继续重连

    async def _run_connection(self):
        """单次连接的运行逻辑"""
        url = build_subscription_url(self.base_ws_url, self.symbol, self.period)
        logger.info(f"WS 连接中：{self.symbol} {self.period} -> {url}")
        
        # 连接超时控制
        try:
            # 依赖 websockets 内置的 ping 机制，不再手动心跳
            connect_options = {
                "close_timeout": 10,
                "max_queue": 1024,
                "ping_interval": 20,  # 每20秒发送一次ping
                "ping_timeout": 20,   # 20秒内未收到pong则认为连接断开
            }
            async with asyncio.timeout(30.0):  # 30秒连接超时
                async with websockets.connect(url, **connect_options) as ws:
                    await self._handle_connection(ws)
                    
        except asyncio.TimeoutError:
            raise Exception("WebSocket连接超时")

    async def _handle_connection(self, ws):
        """处理WebSocket连接"""
        self._last_msg_ts = time.time()
        logger.info(f"WS连接已建立: {self.symbol} {self.period}")
        
        # 移除自定义心跳任务
        try:
            while not self._stop:
                try:
                    # 等待消息，超时时间可以设置得更长，因为有ping/pong保活
                    msg = await asyncio.wait_for(
                        ws.recv(), 
                        timeout=self.heartbeat_timeout_ms / 1000.0 + 5 # 比ping interval稍长
                    )
                    
                    self._last_msg_ts = time.time()
                    await self._handle_message(msg)
                    
                except asyncio.TimeoutError:
                    # 这个超时现在意味着：在指定时间内（例如65s）既没有K线数据，ping/pong也失败了
                    logger.warning(f"WS 消息接收超时 (或连接丢失)：{self.symbol} {self.period}")
                    break # 退出并重连
                    
                except websockets.exceptions.ConnectionClosed as e:
                    logger.warning(f"WS 连接关闭：{self.symbol} {self.period} 代码={e.code}")
                    break
                    
        finally:
            logger.debug(f"WS 连接处理器退出: {self.symbol} {self.period}")

    async def _batch_writer_loop(self):
        """后台任务，批量写入K线并发布事件"""
        while not self._stop:
            try:
                await asyncio.sleep(1.5)  # 每1.5秒检查一次
                
                if not self._kline_batch:
                    continue

                batch_to_write = list(self._kline_batch)
                self._kline_batch.clear()

                try:
                    # 写入数据库
                    await self._insert_klines_direct(batch_to_write)
                    
                    # 发布事件 (如果 event_bus 存在)
                    if self.event_bus:
                        # 确保发布不会阻塞
                        asyncio.create_task(
                            self.event_bus.publish(self.table_name, batch_to_write),
                            name=f"publish_{self.table_name}"
                        )
                except Exception as e:
                    logger.error(f"WS数据直接写入失败: {self.table_name} {e}")
                    # 失败的数据可以考虑放回队列重试，但暂时只记录日志
                    
            except asyncio.CancelledError:
                logger.info(f"WS 写入循环被取消: {self.table_name}")
                break
            except Exception as e:
                logger.error(f"WS 写入循环异常: {self.table_name} {e}")
                
    async def _insert_klines_direct(self, klines: list[Kline]):
        """直接使用 write_client 插入K线"""
        if not klines:
            return
        
        data = [
            [
                k.open_time_ms, k.close_time_ms, str(k.open), str(k.high), str(k.low), str(k.close),
                str(k.volume), str(k.quote_volume), k.count, str(k.taker_buy_base_volume), str(k.taker_buy_quote_volume)
            ]
            for k in klines
        ]
        
        column_names = [
            "open_time", "close_time", "open", "high", "low", "close",
            "volume", "quote_volume", "count", "taker_buy_base_volume", "taker_buy_quote_volume"
        ]
        
        await self.write_client.insert(self.table_name, data, column_names=column_names)
        logger.debug(f"WS 直写 {len(klines)} 条数据到 {self.table_name}")

    async def _handle_message(self, msg: str | bytes):
        try:
            if isinstance(msg, bytes):
                obj_text = msg.decode("utf-8", errors="ignore")
            else:
                obj_text = msg
            obj = json.loads(obj_text)
            k = obj.get("k")
            if not k:
                return
            if not k.get("x", False):
                return

            kline = Kline(
                open_time_ms=int(k["t"]),
                close_time_ms=int(k["T"]),
                open=Decimal(k["o"]),
                high=Decimal(k["h"]),
                low=Decimal(k["l"]),
                close=Decimal(k["c"]),
                volume=Decimal(k["v"]),
                quote_volume=Decimal(k.get("q", "0")),
                count=int(k.get("n", 0)),
                taker_buy_base_volume=Decimal(k.get("V", "0")),
                taker_buy_quote_volume=Decimal(k.get("Q", "0")),
            )
            self._kline_batch.append(kline)
        except Exception as e:
             logger.error("WS 消息处理失败：{} {} 错误={}", self.symbol, self.period, str(e))