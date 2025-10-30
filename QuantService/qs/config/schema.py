from __future__ import annotations
from pydantic import BaseModel, Field


class ClickHouseConfig(BaseModel):
    host: str
    port: int = 8123
    database: str
    username: str
    password: str = ""
    timeout_ms: int = 10000


class MarketConfig(BaseModel):
    ws_url: str
    rest_url: str  # 完整 klines 端点


class BinanceConfig(BaseModel):
    assets: list[str]
    spot: MarketConfig
    um: MarketConfig
    cm: MarketConfig
    api_max_limit: int = 1000
    requests_per_second: int = 15


class BufferConfig(BaseModel):
    batch_size: int = 1000
    flush_interval_ms: int = 2000


class ReconnectConfig(BaseModel):
    initial_backoff_ms: int = 500
    max_backoff_ms: int = 10000


class WebSocketConfig(BaseModel):
    reconnect: ReconnectConfig = Field(default_factory=ReconnectConfig)
    heartbeat_timeout_ms: int = 30000


class BackfillConfig(BaseModel):
    concurrency_windows: int = 4
    window_minutes: int = 60
    start_date: str


class AppConfig(BaseModel):
    environment: str = "prod"
    clickhouse: ClickHouseConfig
    binance: BinanceConfig
    buffer: BufferConfig = Field(default_factory=BufferConfig)
    websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
    backfill: BackfillConfig = Field(default_factory=BackfillConfig)