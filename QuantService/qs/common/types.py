from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal


class MarketType(str, Enum):
    spot = "spot"
    um = "um"
    cm = "cm"


@dataclass
class Kline:
    # 时间戳毫秒（ClickHouse: UInt64）
    open_time_ms: int
    close_time_ms: int
    # OHLC / 量（ClickHouse: Decimal128(8)）
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    # 交易次数（ClickHouse: UInt64；字段名为 count）
    count: int
    # 买入量（taker）
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal

def build_market_symbol(asset: str, market: str | MarketType) -> str:
    m = market.value if isinstance(market, MarketType) else str(market).lower()
    base = asset.upper().replace("-", "").replace("/", "")
    if m == "cm":
        return f"{base}USD_PERP"
    else:
        return f"{base}USDT"