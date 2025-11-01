from __future__ import annotations
from typing import Optional, Dict
from ..common.types import MarketType
from ..config.loader import get_config

def rest_url(market: MarketType) -> str:
    cfg = get_config().binance
    if market == MarketType.spot:
        return cfg.spot.rest_url
    elif market == MarketType.um:
        return cfg.um.rest_url
    else:
        return cfg.cm.rest_url

def step_ms(period: str) -> int:
    if period == "1m":
        return 60_000
    elif period == "1h":
        return 3_600_000
    raise ValueError(f"Unsupported period: {period}")

def build_params(symbol: str, period: str, start_ms: int, end_ms: Optional[int], limit: int) -> Dict[str, int | str]:
    p: Dict[str, int | str] = {
        "symbol": symbol.upper(),
        "interval": period,
        "startTime": start_ms,
        "limit": limit,
    }
    if end_ms is not None:
        p["endTime"] = end_ms
    return p