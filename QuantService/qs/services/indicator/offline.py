from __future__ import annotations
from ...db.client import AsyncClickHouseClient
from ...db.schema import kline_table_name
from ...db.queries import insert_indicator_ma_incremental

class IndicatorOfflineService:
    def __init__(self, client: AsyncClickHouseClient):
        self.client = client

    def compute_batch(self, symbol: str, market: str, period: str, start_ms: int, end_ms: int) -> None:
        table = kline_table_name(symbol, market, period)
        insert_indicator_ma_incremental(self.client, table, symbol, market, period, start_ms, end_ms)