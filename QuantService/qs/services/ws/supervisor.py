from __future__ import annotations
from typing import Dict
from loguru import logger
from ...config.schema import AppConfig
from ...db.schema import kline_table_name
from ...db.client import AsyncClickHouseClient
from ...gateways.binance_ws import ws_base_url
from ...common.types import MarketType
from .upstream import UpstreamStream
from ...db.queries import get_enabled_pairs
from .event_bus import EventBus

class WebSocketSupervisor:
    def __init__(self, cfg: AppConfig, ch_read_client: AsyncClickHouseClient, ch_write_client: AsyncClickHouseClient, event_bus: EventBus | None = None):
        self.cfg = cfg
        self.read_client = ch_read_client
        self.write_client = ch_write_client
        self.streams: Dict[str, UpstreamStream] = {}
        self.status: Dict[str, str] = {}
        self.event_bus = event_bus

    async def start_stream(self, market: str, symbol: str, period: str):
        key = f"{market}|{symbol}|{period}".lower()
        if key in self.streams:
            return
        m = MarketType(market)
        base = ws_base_url(self.cfg, m)
        table = kline_table_name(symbol, market, period)
        stream = UpstreamStream(
            base_ws_url=base,
            symbol=symbol,
            period=period,
            write_client=self.write_client,      # 传入 write_client
            table_name=table,                    # 传入 table_name
            event_bus=self.event_bus,            # 传入 event_bus
            heartbeat_timeout_ms=60000,
            initial_backoff_ms=500,
            max_backoff_ms=10000,
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
        pairs = await get_enabled_pairs(self.read_client)
        for symbol, market in pairs:
            await self.start_stream(market, symbol, "1m")
            await self.start_stream(market, symbol, "1h")

    def snapshot(self) -> Dict[str, str]:
        return dict(self.status)