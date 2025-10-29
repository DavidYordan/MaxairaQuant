from __future__ import annotations
from typing import Dict
from loguru import logger
from ...config.schema import AppConfig
from ...db.schema import ensure_kline_table, kline_table_name
from ...db.client import Client
from ...buffer.buffer import DataBuffer
from ...gateways.binance_ws import ws_base_url
from ...common.types import MarketType
from .upstream import UpstreamStream
from ...db.queries import get_enabled_pairs

class WebSocketSupervisor:
    def __init__(self, cfg: AppConfig, ch_client: Client):
        self.cfg = cfg
        self.client = ch_client
        self.streams: Dict[str, UpstreamStream] = {}
        self.buffers: Dict[str, DataBuffer] = {}
        self.status: Dict[str, str] = {}

    async def start_stream(self, market: str, symbol: str, period: str):
        key = f"{market}|{symbol}|{period}".lower()
        if key in self.streams:
            return
        m = MarketType(market)
        base = ws_base_url(self.cfg, m)
        table = kline_table_name(symbol, market, period)
        ensure_kline_table(self.client, table)
        buf = self.buffers.get(key)
        if not buf:
            buf = DataBuffer(self.client, table, self.cfg.buffer.batch_size, self.cfg.buffer.flush_interval_ms)
            self.buffers[key] = buf
            await buf.start()
        stream = UpstreamStream(
            base_ws_url=base,
            symbol=symbol,
            period=period,
            buffer=buf,
            heartbeat_timeout_ms=self.cfg.websocket.heartbeat_timeout_ms,
            initial_backoff_ms=self.cfg.websocket.reconnect.initial_backoff_ms,
            max_backoff_ms=self.cfg.websocket.reconnect.max_backoff_ms,
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
        buf = self.buffers.pop(key, None)
        if buf:
            await buf.stop()
        logger.info("WS 流已停止：{}", key)

    async def start_enabled_streams(self):
        pairs = get_enabled_pairs(self.client)
        for symbol, market in pairs:
            await self.start_stream(market, symbol, "1m")
            await self.start_stream(market, symbol, "1h")

    def snapshot(self) -> Dict[str, str]:
        return dict(self.status)