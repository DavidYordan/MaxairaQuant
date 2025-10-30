from __future__ import annotations
import asyncio
from typing import List, Tuple, Dict
from clickhouse_connect.driver.client import Client
from ...db.schema import kline_table_name
from ...db.queries import get_max_open_time
from ...gateways.binance_rest import step_ms

class IndicatorOnlineService:
    def __init__(self, client: Client, poll_interval_ms: int = 2000):
        self.client = client
        self.poll_interval_ms = poll_interval_ms
        self._tasks: List[asyncio.Task] = []
        self._cursors: Dict[str, int] = {}

    async def start(self, pairs: List[Tuple[str, str]], periods: List[str]) -> None:
        for symbol, market in pairs:
            for period in periods:
                table = kline_table_name(symbol, market, period)
                # 初始游标：派生表或源表的最大 open_time
                self._cursors[table] = max(get_max_open_time(self.client, table), self._cursors.get(table, 0))
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
                latest = get_max_open_time(self.client, table)
                cursor = self._cursors.get(table, 0)
                if latest > cursor:
                    start_ms = max(0, cursor - preheat_ms)
                    # 移除增量 INSERT，游标推进由物化视图负责指标写入
                    self._cursors[table] = latest
                await asyncio.sleep(self.poll_interval_ms / 1000.0)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(self.poll_interval_ms / 1000.0)

    async def _insert_incremental(self, symbol: str, market: str, period: str, start_ms: int, end_ms: int) -> None:
        table = kline_table_name(symbol, market, period)
        await asyncio.to_thread(
            self.client.query,
            f"""
            INSERT INTO indicator_ma (symbol, market, period, open_time, ma20, ma50)
            SELECT
              '{symbol}' AS symbol,
              '{market}' AS market,
              '{period}' AS period,
              open_time,
              avg(toFloat64(close)) OVER (ORDER BY open_time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
              avg(toFloat64(close)) OVER (ORDER BY open_time ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50
            FROM {table}
            WHERE open_time >= %(s)s AND open_time <= %(e)s
            """,
            query_parameters={"s": start_ms, "e": end_ms},
        )