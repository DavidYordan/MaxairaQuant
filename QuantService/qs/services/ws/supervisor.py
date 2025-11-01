from __future__ import annotations
import asyncio
from typing import Dict
from loguru import logger
from ...config.loader import get_config
from ...db.schema import kline_table_name
from ...gateways.binance_ws import ws_base_url
from ...common.types import MarketType
from .upstream import UpstreamStream
from ...db.queries import get_enabled_pairs
from .event_bus import EventBus

class WebSocketSupervisor:
    def __init__(self, event_bus: EventBus):
        cfg = get_config().websocket
        self.streams: Dict[str, UpstreamStream] = {}
        self.status: Dict[str, str] = {}
        self.event_bus = event_bus
        self.heartbeat_timeout_ms = cfg.heartbeat_timeout_ms
        self.reconnect_initial_backoff_ms = cfg.reconnect.initial_backoff_ms
        self.reconnect_max_backoff_ms = cfg.reconnect.max_backoff_ms

    async def start_stream(self, market: str, symbol: str, period: str):
        key = f"{market}|{symbol}|{period}".lower()
        if key in self.streams:
            return
        m = MarketType(market)
        base = ws_base_url(m)
        table = kline_table_name(symbol, market, period)
        stream = UpstreamStream(
            base_ws_url=base,
            symbol=symbol,
            period=period,
            table_name=table,
            event_bus=self.event_bus,
            heartbeat_timeout_ms=self.heartbeat_timeout_ms,
            initial_backoff_ms=self.reconnect_initial_backoff_ms,
            max_backoff_ms=self.reconnect_max_backoff_ms,
        )
        self.streams[key] = stream
        await stream.start()
        self.status[key] = "running"
        logger.info("WS 流已启动：{}", key)

    async def stop_stream(self, market: str, symbol: str, period: str):
        key = f"{market}|{symbol}|{period}".lower()
        stream = self.streams.pop(key, None)
        if stream:
            await stream.stop()
            self.status[key] = "stopped"
        logger.info("WS 流已停止：{}", key)

    async def start_enabled_streams(self):
        # 内部拉取客户端并查询启用交易对

        pairs = await get_enabled_pairs()
        for symbol, market in pairs:
            await self.start_stream(market, symbol, "1m")
            await self.start_stream(market, symbol, "1h")

    async def stop_all_streams(self):
        """优雅停止所有正在运行的WS流"""
        if not self.streams:
            return
        tasks = []
        for key, stream in list(self.streams.items()):
            tasks.append(asyncio.create_task(stream.stop(), name=f"stop_{key}"))
            self.status[key] = "stopped"
        self.streams.clear()
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                logger.warning("部分WS流停止超时")

    def snapshot(self) -> Dict[str, str]:
        return dict(self.status)