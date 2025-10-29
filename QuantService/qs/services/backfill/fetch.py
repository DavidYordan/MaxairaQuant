from __future__ import annotations
from typing import Optional, List, Tuple
from decimal import Decimal
import httpx
from ...common.types import MarketType, Kline
from ...config.schema import AppConfig
from ...gateways.binance_rest import rest_url, build_params

async def fetch_klines(
    cfg: AppConfig,
    market: MarketType,
    symbol: str,
    period: str,
    start_ms: int,
    end_ms: Optional[int],
    limit: int,
    proxy_url: Optional[str],
) -> Tuple[List[Kline], dict]:
    url = rest_url(cfg, market)
    params = build_params(symbol, period, start_ms, end_ms, limit)
    proxies = {"all://": proxy_url} if proxy_url else None
    timeout = httpx.Timeout(10.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout, proxies=proxies) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        headers = dict(r.headers)

    klines: List[Kline] = []
    for item in data:
        # [openTime, open, high, low, close, volume, closeTime, quoteAssetVolume, numberOfTrades, takerBuyBaseVolume, takerBuyQuoteVolume, ignore]
        klines.append(
            Kline(
                open_time_ms=int(item[0]),
                close_time_ms=int(item[6]),
                open=Decimal(item[1]),
                high=Decimal(item[2]),
                low=Decimal(item[3]),
                close=Decimal(item[4]),
                volume=Decimal(item[5]),
                quote_volume=Decimal(item[7]),
                count=int(item[8]),
                taker_buy_base_volume=Decimal(item[9]),
                taker_buy_quote_volume=Decimal(item[10]),
            )
        )
    return klines, headers