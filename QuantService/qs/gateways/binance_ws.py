from __future__ import annotations
from ..common.types import MarketType
from ..config.loader import get_config

def ws_base_url(market: MarketType) -> str:
    cfg = get_config().binance
    if market == MarketType.spot:
        return cfg.spot.ws_url
    elif market == MarketType.um:
        return cfg.um.ws_url
    else:
        return cfg.cm.ws_url

def build_subscription_url(base: str, symbol: str, period: str) -> str:
    if not base.endswith("/"):
        base = base + "/"
    return f"{base}{symbol.lower()}@kline_{period}"