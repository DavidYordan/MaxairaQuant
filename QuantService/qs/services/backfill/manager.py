from __future__ import annotations
import asyncio
from typing import List, Optional, Tuple, Dict
from loguru import logger
from buffer.buffer import DataBuffer
from common.types import MarketType
from config.schema import AppConfig
from gateways.binance_rest import step_ms
from services.backfill.rate_limiter import ApiRateLimiter
from services.backfill.fetch import fetch_klines
from services.proxy.registry import get_enabled_proxy_url
from db.schema import ensure_kline_table, kline_table_name
from monitoring.metrics import Metrics

class BackfillManager:
    def __init__(self, cfg: AppConfig, ch_client):
        self.cfg = cfg
        self.client = ch_client
        self.limiter = ApiRateLimiter(cfg.binance.requests_per_second)
        self._markets_using_proxy: Dict[MarketType, bool] = {MarketType.spot: False, MarketType.um: False, MarketType.cm: False}
        self._window_concurrency = cfg.backfill.concurrency_windows
        self.metrics = Metrics()
        # 统一写入缓冲：每表一个 DataBuffer
        self._buffers: Dict[str, DataBuffer] = {}

    async def start(self):
        await self.limiter.start()

    async def stop(self):
        await self.limiter.stop()

    def _window_size_ms(self, period: str) -> int:
        return step_ms(period) * self.cfg.binance.api_max_limit

    def _split_gap(self, start_ms: int, end_ms: int, period: str) -> List[Tuple[int, int]]:
        size = self._window_size_ms(period)
        windows: List[Tuple[int, int]] = []
        cur = start_ms
        while cur <= end_ms:
            nxt = min(cur + size - 1, end_ms)
            windows.append((cur, nxt))
            cur = nxt + 1
        return windows

    def _select_proxy_url(self, market: MarketType) -> Optional[str]:
        if self._markets_using_proxy.get(market, False):
            return get_enabled_proxy_url(self.client)
        return None

    def enable_proxy_for_market(self, market: MarketType):
        self._markets_using_proxy[market] = True
        logger.warning("启用代理用于市场：{}", market)

    async def backfill_gap(self, market: MarketType, symbol: str, period: str, gap_start_ms: int, gap_end_ms: int):
        table = kline_table_name(symbol, market.value, period)
        ensure_kline_table(self.client, table)
        windows = self._split_gap(gap_start_ms, gap_end_ms, period)
        sem = asyncio.Semaphore(self._window_concurrency)
        tasks: List[asyncio.Task] = []
        for (ws, we) in windows:
            tasks.append(asyncio.create_task(self._do_window(sem, market, symbol, period, table, ws, we)))
        await asyncio.gather(*tasks)

    async def _ensure_buffer(self, table: str):
        buf = self._buffers.get(table)
        if not buf:
            buf = DataBuffer(self.client, table, self.cfg.buffer.batch_size, self.cfg.buffer.flush_interval_ms)
            self._buffers[table] = buf
            await buf.start()
        return buf

    async def _do_window(self, sem: asyncio.Semaphore, market: MarketType, symbol: str, period: str, table: str, ws: int, we: int):
        async with sem:
            await self.limiter.acquire()
            proxy_url = self._select_proxy_url(market)
            self.metrics.inc_requests()
            try:
                klines, headers = await fetch_klines(self.cfg, market, symbol, period, ws, we, self.cfg.binance.api_max_limit, proxy_url)
                self.limiter.update_from_headers(headers)
                await self.limiter.maybe_block_by_minute_weight()
                buf = await self._ensure_buffer(table)
                await buf.add_many(klines)
                self.metrics.inc_successes()
                self.metrics.add_inserted_rows(len(klines))
                logger.info("窗口成功：{} {} {} [{},{}] 写入 {} 行", market, symbol, period, ws, we, len(klines))
            except Exception as e:
                code = getattr(e, "response", None)
                status = code.status_code if code is not None else None
                if status in (403, 451):
                    self.enable_proxy_for_market(market)
                    await asyncio.sleep(0.5)
                    await self._retry_with_proxy(market, symbol, period, table, ws, we)
                elif status == 429:
                    retry_after = 1.0
                    try:
                        retry_after = float(e.response.headers.get("Retry-After", "1"))
                    except Exception:
                        pass
                    await asyncio.sleep(retry_after)
                    await self._retry_with_proxy(market, symbol, period, table, ws, we)
                else:
                    logger.error("窗口失败：{} {} {} [{},{}] 状态={} 错误={}", market, symbol, period, ws, we, status, str(e))
            finally:
                self.limiter.release()

    async def _retry_with_proxy(self, market: MarketType, symbol: str, period: str, table: str, ws: int, we: int):
        await asyncio.sleep(0.5)
        proxy_url = self._select_proxy_url(market)
        try:
            klines, headers = await fetch_klines(self.cfg, market, symbol, period, ws, we, self.cfg.binance.api_max_limit, proxy_url)
            self.limiter.update_from_headers(headers)
            await self.limiter.maybe_block_by_minute_weight()
            buf = await self._ensure_buffer(table)
            await buf.add_many(klines)
            self.metrics.inc_successes()
            self.metrics.add_inserted_rows(len(klines))
            logger.info("代理重试成功：{} {} {} [{},{}] 写入 {} 行", market, symbol, period, ws, we, len(klines))
        except Exception as e:
            code = getattr(e, "response", None)
            status = code.status_code if code is not None else None
            self.metrics.inc_failures()
            logger.error("代理重试失败：{} {} {} [{},{}] 状态={} 错误={}", market, symbol, period, ws, we, status, str(e))