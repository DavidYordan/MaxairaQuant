from __future__ import annotations
from typing import Optional, List, Tuple
from decimal import Decimal
import httpx
from ...common.types import MarketType, Kline
from ...config.schema import AppConfig
from ...gateways.binance_rest import rest_url, build_params

# 连接池：按 proxy_url 维度复用 AsyncClient
_CLIENTS: dict[str, httpx.AsyncClient] = {}

def _client_key(proxy_url: Optional[str]) -> str:
    return proxy_url or "DIRECT"

def _build_client(proxy_url: Optional[str]) -> httpx.AsyncClient:
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    timeout = httpx.Timeout(10.0, connect=10.0)
    # 使用 transport 指定代理，兼容 socks
    transport = httpx.AsyncHTTPTransport(proxy=proxy_url, retries=2) if proxy_url else httpx.AsyncHTTPTransport(retries=2)
    return httpx.AsyncClient(timeout=timeout, transport=transport, limits=limits, http2=True)

def _get_client(proxy_url: Optional[str]) -> httpx.AsyncClient:
    key = _client_key(proxy_url)
    cli = _CLIENTS.get(key)
    if cli is None:
        cli = _build_client(proxy_url)
        _CLIENTS[key] = cli
    return cli

async def aclose_all_clients() -> None:
    # 统一回收，避免资源泄漏
    for key, cli in list(_CLIENTS.items()):
        try:
            await cli.aclose()
        except Exception:
            pass
        finally:
            _CLIENTS.pop(key, None)

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

    client = _get_client(proxy_url)
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