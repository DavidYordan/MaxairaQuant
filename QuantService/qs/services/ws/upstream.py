from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Optional, List

import websockets
from loguru import logger

from ...common.types import Kline
from ...db.queries import insert_klines
from ...gateways.binance_ws import build_subscription_url
from ...services.ws.event_bus import EventBus


class UpstreamStream:
    def __init__(
        self,
        base_ws_url: str,
        symbol: str,
        period: str,
        table_name: str,
        event_bus: EventBus,
        heartbeat_timeout_ms: int = 60000,
        initial_backoff_ms: int = 500,
        max_backoff_ms: int = 10000,
    ) -> None:
        self.base_ws_url = base_ws_url
        self.symbol = symbol
        self.period = period
        self.table_name = table_name
        self.event_bus = event_bus
        self.heartbeat_timeout_ms = heartbeat_timeout_ms
        self.initial_backoff_ms = initial_backoff_ms
        self.max_backoff_ms = max_backoff_ms

        # ws 主任务
        self._task: Optional[asyncio.Task] = None
        # 批量写入任务
        self._batch_writer_task: Optional[asyncio.Task] = None

        self._stop = False
        self._last_msg_ts: float = 0.0

        # 批量缓冲 & 锁
        self._kline_batch: List[Kline] = []
        self._batch_lock = asyncio.Lock()

    # -------------------------------------------------------------------------
    # lifecycle
    # -------------------------------------------------------------------------
    async def start(self) -> None:
        """启动 WebSocket + 写入循环"""
        if self._task is not None:
            logger.warning(f"WS连接已在运行: {self.symbol} {self.period}")
            return

        self._stop = False

        self._task = asyncio.create_task(
            self._run_with_supervision(),
            name=f"ws_{self.symbol}_{self.period}",
        )
        self._batch_writer_task = asyncio.create_task(
            self._batch_writer_loop(),
            name=f"wswriter_{self.symbol}_{self.period}",
        )
        logger.info(f"WS连接任务已启动: {self.symbol} {self.period}")

    async def stop(self) -> None:
        """优雅停止（不管两个任务是不是还在，都要关掉）"""
        logger.info(f"正在停止WS连接: {self.symbol} {self.period}")
        self._stop = True

        tasks: List[asyncio.Task] = []
        if self._task is not None:
            self._task.cancel()
            tasks.append(self._task)
        if self._batch_writer_task is not None:
            self._batch_writer_task.cancel()
            tasks.append(self._batch_writer_task)

        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                logger.warning(f"WS连接停止超时: {self.symbol} {self.period}")

        self._task = None
        self._batch_writer_task = None
        logger.info(f"WS连接已停止: {self.symbol} {self.period}")

    # -------------------------------------------------------------------------
    # supervision
    # -------------------------------------------------------------------------
    async def _run_with_supervision(self) -> None:
        """带监督的运行逻辑：断了就按指数退避重连"""
        consecutive_failures = 0
        max_consecutive_failures = 10

        while not self._stop:
            try:
                await self._run_connection()
                # 能正常跑完一次（一般是 ws 自己断了）就重置失败次数
                consecutive_failures = 0

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

                backoff_time = min(
                    self.initial_backoff_ms * (2 ** (consecutive_failures - 1)) / 1000.0,
                    self.max_backoff_ms / 1000.0,
                )

                if not self._stop:
                    logger.info(
                        f"WS将在 {backoff_time:.1f}s 后重连: {self.symbol} {self.period}"
                    )
                    try:
                        # 3.10 用 wait_for 包一个永不 set 的事件来 sleep
                        await asyncio.wait_for(
                            asyncio.Event().wait(),
                            timeout=backoff_time,
                        )
                    except asyncio.TimeoutError:
                        # 正常超时，继续下一轮
                        pass

    # -------------------------------------------------------------------------
    # single connection
    # -------------------------------------------------------------------------
    async def _run_connection(self) -> None:
        """真正的一次连接"""
        url = build_subscription_url(self.base_ws_url, self.symbol, self.period)
        logger.info(f"WS 连接中：{self.symbol} {self.period} -> {url}")

        connect_options = {
            "close_timeout": 10,
            "max_queue": 1024,
            "ping_interval": 20,  # 每20秒ping一次
            "ping_timeout": 10,   # 10秒收不到pong就断
        }

        try:
            ws = await asyncio.wait_for(
                websockets.connect(url, **connect_options),
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            raise Exception("WebSocket连接超时")

        # 用 async with 来确保退出时关闭
        async with ws:
            await self._handle_connection(ws)

    async def _handle_connection(self, ws) -> None:
        """收到消息就处理，保活交给 websockets"""
        self._last_msg_ts = time.time()
        logger.info(f"WS连接已建立: {self.symbol} {self.period}")

        try:
            async for msg in ws:
                self._last_msg_ts = time.time()
                await self._handle_message(msg)

        except asyncio.CancelledError:
            logger.info(f"WS 连接被取消：{self.symbol} {self.period}")
            raise

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WS 连接关闭：{self.symbol} {self.period} 代码={e.code}")

        finally:
            logger.debug(f"WS 连接处理器退出: {self.symbol} {self.period}")

    # -------------------------------------------------------------------------
    # batch writer
    # -------------------------------------------------------------------------
    async def _batch_writer_loop(self) -> None:
        """后台任务：定期把内存里的 K 线批量写入 ClickHouse"""
        while not self._stop:
            try:
                await asyncio.sleep(1.5)

                # 先原子性地“拿走一批”
                async with self._batch_lock:
                    if not self._kline_batch:
                        continue
                    batch_to_write = self._kline_batch
                    self._kline_batch = []

                # 写入数据库
                try:
                    await insert_klines(self.table_name, batch_to_write)
                except Exception as e:
                    logger.error(f"WS数据直接写入失败: {self.table_name} {e}")
                    # 写失败就别发事件了
                    continue

            except asyncio.CancelledError:
                logger.info(f"WS 写入循环被取消: {self.table_name}")
                break
            except Exception as e:
                logger.error(f"WS 写入循环异常: {self.table_name} {e}")
    
    # -------------------------------------------------------------------------
    # message handler
    # -------------------------------------------------------------------------
    async def _handle_message(self, msg: str | bytes) -> None:
        try:
            if isinstance(msg, bytes):
                obj_text = msg.decode("utf-8", errors="ignore")
            else:
                obj_text = msg

            obj = json.loads(obj_text)
            k = obj.get("k")
            if not k:
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

            asyncio.create_task(
                self.event_bus.publish(self.table_name, kline),
                name=f"publish_{self.table_name}",
            )

            if k.get("x", False):
                async with self._batch_lock:
                    self._kline_batch.append(kline)

        except Exception as e:
            logger.error(
                "WS 消息处理失败：{} {} 错误={}",
                self.symbol,
                self.period,
                str(e),
            )
