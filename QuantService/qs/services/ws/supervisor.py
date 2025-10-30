from __future__ import annotations
from typing import Dict
from loguru import logger
from ...config.schema import AppConfig
from ...db.schema import kline_table_name
from ...db.client import AsyncClickHouseClient
from ...buffer.buffer import DataBuffer
from ...gateways.binance_ws import ws_base_url
from ...common.types import MarketType
from .upstream import UpstreamStream
from ...db.queries import get_enabled_pairs
from .event_bus import EventBus

class WebSocketSupervisor:
    def __init__(self, cfg: AppConfig, ch_client: AsyncClickHouseClient, event_bus: EventBus | None = None):
        self.cfg = cfg
        self.client = ch_client
        self.streams: Dict[str, UpstreamStream] = {}
        self.buffers: Dict[str, DataBuffer] = {}
        self.status: Dict[str, str] = {}
        self.event_bus = event_bus

    async def start_stream(self, market: str, symbol: str, period: str):
        key = f"{market}|{symbol}|{period}".lower()
        if key in self.streams:
            return
        m = MarketType(market)
        base = ws_base_url(self.cfg, m)
        table = kline_table_name(symbol, market, period)
        buf = self.buffers.get(key)
        if not buf:
            # 创建缓冲区，使用优化的默认参数
            buf = DataBuffer(
                client=self.client, 
                table_name=table, 
                batch_size=2000,           # 优化的批次大小
                flush_interval_ms=1500,    # 1.5秒刷新间隔
                event_bus=self.event_bus,
                writer_workers=4,          # 4个写入工作器
                max_queue_size=20000       # 2万条队列大小
            )
            self.buffers[key] = buf
            await buf.start()
        stream = UpstreamStream(
            base_ws_url=base,
            symbol=symbol,
            period=period,
            buffer=buf,
            heartbeat_timeout_ms=60000,    # 60秒心跳超时
            initial_backoff_ms=500,        # 500ms初始退避
            max_backoff_ms=10000,          # 10秒最大退避
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
        pairs = await get_enabled_pairs(self.client)
        for symbol, market in pairs:
            await self.start_stream(market, symbol, "1m")
            await self.start_stream(market, symbol, "1h")

    def snapshot(self) -> Dict[str, str]:
        return dict(self.status)