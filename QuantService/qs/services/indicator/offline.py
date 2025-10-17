from __future__ import annotations
from typing import List, Tuple
from clickhouse_connect.driver.client import Client
from db.schema import kline_table_name

class IndicatorOfflineService:
    def __init__(self, client: Client):
        self.client = client

    def ensure_tables(self) -> None:
        self.client.command(
            """
            CREATE TABLE IF NOT EXISTS indicator_ma
            (
              symbol String,
              market String,
              period String,
              open_time UInt64,
              ma20 Float64,
              ma50 Float64,
              created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree
            ORDER BY (symbol, market, period, open_time)
            """
        )

    def ensure_mv_for_table(self, symbol: str, market: str, period: str) -> None:
        table = kline_table_name(symbol, market, period)
        view_name = f"mv_indicator_ma_{symbol.lower()}_{market.lower()}_{period.lower()}"
        self.client.command(
            f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
            TO indicator_ma AS
            SELECT
              '{symbol}' AS symbol,
              '{market}' AS market,
              '{period}' AS period,
              open_time,
              avg(toFloat64(close)) OVER (ORDER BY open_time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
              avg(toFloat64(close)) OVER (ORDER BY open_time ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50
            FROM {table}
            """
        )

    def bootstrap_materialized_views(self, pairs: List[Tuple[str, str]], periods: List[str]) -> None:
        for symbol, market in pairs:
            for period in periods:
                self.ensure_mv_for_table(symbol, market, period)

    def compute_batch(self, symbol: str, market: str, period: str, start_ms: int, end_ms: int) -> None:
        table = kline_table_name(symbol, market, period)
        self.client.command(
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
            params={"s": start_ms, "e": end_ms},
        )