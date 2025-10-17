from __future__ import annotations
import asyncio
import json
import time
from decimal import Decimal
import websockets
from loguru import logger
from buffer.buffer import DataBuffer
from gateways.binance_ws import build_subscription_url
from common.types import Kline

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
        if self._task is None:
            self._stop = False
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()
            self._task = None

    async def _run(self):
        backoff_ms = self.initial_backoff_ms
        url = build_subscription_url(self.base_ws_url, self.symbol, self.period)
        while not self._stop:
            try:
                logger.info("WS 连接中：{} {} -> {}", self.symbol, self.period, url)
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1024,
                ) as ws:
                    self._last_msg_ts = time.time()
                    backoff_ms = self.initial_backoff_ms
                    while not self._stop:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=self.heartbeat_timeout_ms / 1000.0)
                            self._last_msg_ts = time.time()
                            await self._handle_message(msg)
                        except asyncio.TimeoutError:
                            logger.warning("WS 心跳超时，准备重连：{} {}", self.symbol, self.period)
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("WS 连接异常：{} {} 错误={}", self.symbol, self.period, str(e))
            await asyncio.sleep(backoff_ms / 1000.0)
            backoff_ms = min(backoff_ms * 2, self.max_backoff_ms)

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