from __future__ import annotations
from common.types import MarketType
from config.schema import AppConfig

def ws_base_url(cfg: AppConfig, market: MarketType) -> str:
    if market == MarketType.spot:
        return cfg.binance.spot.ws_url
    elif market == MarketType.um:
        return cfg.binance.um.ws_url
    else:
        return cfg.binance.cm.ws_url

def build_subscription_url(base: str, symbol: str, period: str) -> str:
    if not base.endswith("/"):
        base = base + "/"
    return f"{base}{symbol.lower()}@kline_{period}"