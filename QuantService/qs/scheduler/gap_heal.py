from __future__ import annotations
import asyncio
import time
from typing import Dict, List, Tuple
from loguru import logger
from config.schema import AppConfig
from db.schema import kline_table_name, ensure_kline_table
from db.queries import get_enabled_pairs, find_gaps_windowed_sql
from services.backfill.manager import BackfillManager
from gateways.binance_rest import step_ms
from common.types import MarketType

class GapHealScheduler:
    def __init__(self, cfg: AppConfig, ch_client, backfill: BackfillManager):
        self.cfg = cfg
        self.client = ch_client
        self.backfill = backfill
        self._tasks: List[asyncio.Task] = []
        self._running_keys: Dict[str, bool] = {}

    async def start(self):
        # 1m: 每 30s 扫描
        self._tasks.append(asyncio.create_task(self._loop(period="1m", interval_s=30)))
        # 1h: 每 5min 扫描
        self._tasks.append(asyncio.create_task(self._loop(period="1h", interval_s=300)))
        logger.info("GapHealScheduler 已启动（1m/30s，1h/300s）")

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    async def _loop(self, period: str, interval_s: int):
        s_ms = step_ms(period)
        while True:
            try:
                end_ms = int(time.time() * 1000)
                start_ms = end_ms - self.cfg.backfill.window_minutes * s_ms
                pairs = get_enabled_pairs(self.client)
                for symbol, market in pairs:
                    # 双周期：本循环固定 period
                    table = kline_table_name(symbol, market, period)
                    ensure_kline_table(self.client, table)
                    gaps = find_gaps_windowed_sql(self.client, table, start_ms, end_ms, s_ms)
                    if gaps:
                        logger.info("发现缺口：{} {} {} 数量={}", market, symbol, period, len(gaps))
                    for (gs, ge) in gaps:
                        key = f"{market}|{symbol}|{period}"
                        if self._running_keys.get(key, False):
                            continue
                        self._running_keys[key] = True
                        asyncio.create_task(self._dispatch_and_release(key, market, symbol, period, gs, ge))
                await asyncio.sleep(interval_s)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("GapHealScheduler 异常：{}", str(e))
                await asyncio.sleep(interval_s)

    async def _dispatch_and_release(self, key: str, market: str, symbol: str, period: str, gs: int, ge: int):
        try:
            await self.backfill.backfill_gap(MarketType(market), symbol, period, gs, ge)
        finally:
            self._running_keys[key] = False