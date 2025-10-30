from __future__ import annotations
import asyncio
import json
import time
from decimal import Decimal
import websockets
from loguru import logger
from ...buffer.buffer import DataBuffer
from ...gateways.binance_ws import build_subscription_url
from ...common.types import Kline

class UpstreamStream:
    def __init__(self, base_ws_url: str, symbol: str, period: str, buffer: DataBuffer, heartbeat_timeout_ms: int, initial_backoff_ms: int, max_backoff_ms: int):
        self.base_ws_url = base_ws_url
        self.symbol = symbol
        self.period = period
        self.buffer = buffer
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
        logger.info(f"WS连接任务已启动: {self.symbol} {self.period}")

    async def stop(self):
        """优雅停止WebSocket连接"""
        if self._task is None:
            return
            
        logger.info(f"正在停止WS连接: {self.symbol} {self.period}")
        self._stop = True
        
        # 取消任务并等待完成
        self._task.cancel()
        try:
            await asyncio.wait_for(self._task, timeout=10.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            logger.warning(f"WS连接停止超时: {self.symbol} {self.period}")
        
        self._task = None
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
            async with asyncio.timeout(30.0):  # 30秒连接超时
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1024,
                ) as ws:
                    await self._handle_connection(ws)
                    
        except asyncio.TimeoutError:
            raise Exception("WebSocket连接超时")

    async def _handle_connection(self, ws):
        """处理WebSocket连接"""
        self._last_msg_ts = time.time()
        logger.info(f"WS连接已建立: {self.symbol} {self.period}")
        
        # 创建心跳监控任务
        heartbeat_task = asyncio.create_task(
            self._heartbeat_monitor(),
            name=f"heartbeat_{self.symbol}_{self.period}"
        )
        
        try:
            while not self._stop:
                try:
                    # 等待消息，带超时
                    msg = await asyncio.wait_for(
                        ws.recv(), 
                        timeout=self.heartbeat_timeout_ms / 1000.0
                    )
                    
                    self._last_msg_ts = time.time()
                    await self._handle_message(msg)
                    
                except asyncio.TimeoutError:
                    logger.warning(f"WS 心跳超时，准备重连：{self.symbol} {self.period}")
                    break
                    
                except websockets.exceptions.ConnectionClosed as e:
                    logger.warning(f"WS 连接关闭：{self.symbol} {self.period} 代码={e.code}")
                    break
                    
        finally:
            # 清理心跳任务
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_monitor(self):
        """心跳监控任务"""
        while not self._stop:
            try:
                await asyncio.sleep(10.0)  # 每10秒检查一次
                
                current_time = time.time()
                time_since_last_msg = current_time - self._last_msg_ts
                
                if time_since_last_msg > (self.heartbeat_timeout_ms / 1000.0):
                    logger.warning(
                        f"WS心跳检测失败: {self.symbol} {self.period} "
                        f"距离上次消息 {time_since_last_msg:.1f}s"
                    )
                    break
                    
            except asyncio.CancelledError:
                break

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
            if not k.get("x", False):  # 仅写闭合K线
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
            await self.buffer.add(kline)
        except Exception as e:
            logger.error("WS 消息处理失败：{} {} 错误={}", self.symbol, self.period, str(e))