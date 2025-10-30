from __future__ import annotations
import asyncio
from typing import List, Tuple, Dict
from ...db.client import AsyncClickHouseClient
from ...db.schema import kline_table_name
from ...db.queries import get_max_open_time
from ...gateways.binance_rest import step_ms
from ...db.queries import insert_indicator_ma_incremental

class IndicatorOnlineService:
    def __init__(self, client: AsyncClickHouseClient, poll_interval_ms: int = 2000):
        self.client = client
        self.poll_interval_ms = poll_interval_ms
        self._tasks: List[asyncio.Task] = []
        self._cursors: Dict[str, int] = {}

    async def start(self, pairs: List[Tuple[str, str]], periods: List[str]) -> None:
        for symbol, market in pairs:
            for period in periods:
                table = kline_table_name(symbol, market, period)
                # 初始游标：派生表或源表的最大 open_time
                max_time = await get_max_open_time(self.client, table)
                self._cursors[table] = max(max_time, self._cursors.get(table, 0))
                self._tasks.append(asyncio.create_task(self._loop(symbol, market, period)))
        return None

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    async def _loop(self, symbol: str, market: str, period: str) -> None:
        table = kline_table_name(symbol, market, period)
        s_ms = step_ms(period)
        preheat_ms = 50 * s_ms
        while True:
            try:
                # 异步获取最新时间戳，避免阻塞
                latest = await get_max_open_time(self.client, table)
                cursor = self._cursors.get(table, 0)
                if latest > cursor:
                    start_ms = max(0, cursor - preheat_ms)
                    # 异步执行指标计算
                    await self._insert_incremental(symbol, market, period, start_ms, latest)
                    self._cursors[table] = latest
                await asyncio.sleep(self.poll_interval_ms / 1000.0)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self.poll_interval_ms / 1000.0)

    async def _insert_incremental(self, symbol: str, market: str, period: str, start_ms: int, end_ms: int) -> None:
        table = kline_table_name(symbol, market, period)
        # insert_indicator_ma_incremental 已经是异步函数，直接调用
        await insert_indicator_ma_incremental(
            self.client,
            table,
            symbol,
            market,
            period,
            start_ms,
            end_ms,
        )