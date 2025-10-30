from __future__ import annotations
import asyncio
from typing import List, Tuple
from clickhouse_connect.driver.client import Client
from ..common.types import Kline


async def get_enabled_pairs(client: Client) -> List[Tuple[str, str]]:
    rs = await asyncio.to_thread(
        client.query,
        "SELECT symbol, market_type FROM trading_pair_config WHERE enabled = 1 ORDER BY symbol, market_type"
    )
    return [(row[0], row[1]) for row in rs.result_rows]


async def get_max_open_time(client: Client, table_name: str) -> int:
    rs = await asyncio.to_thread(client.query, f"SELECT max(open_time) FROM {table_name}")
    val = rs.first_row[0]
    return int(val or 0)


async def insert_klines(client: Client, table_name: str, klines: List[Kline]) -> None:
    if not klines:
        return
    rows = [
        [
            k.open_time_ms,
            k.close_time_ms,
            str(k.open),
            str(k.high),
            str(k.low),
            str(k.close),
            str(k.volume),
            str(k.quote_volume),
            int(k.count),
            str(k.taker_buy_base_volume),
            str(k.taker_buy_quote_volume),
        ]
        for k in klines
    ]
    await asyncio.to_thread(
        client.insert,
        table_name,
        rows,
        column_names=[
            "open_time",
            "close_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        ],
    )


def find_gaps_windowed_sql(
    client: Client,
    table_name: str,
    window_start_ms: int,
    window_end_ms: int,
    step_ms: int,
) -> List[Tuple[int, int]]:
    # 统计、边界
    agg = client.query(
        f"SELECT count() AS c, min(open_time) AS min_ot, max(open_time) AS max_ot FROM {table_name} WHERE open_time >= {window_start_ms} AND open_time <= {window_end_ms}"
    ).first_row
    count = int(agg[0] or 0)
    if count == 0:
        return [(window_start_ms, window_end_ms)]
    min_ot = int(agg[1])
    max_ot = int(agg[2])

    # 中间缺口（lag）
    rs = client.query(
        f"""
        SELECT
          open_time,
          lag(open_time, 1) OVER (ORDER BY open_time) AS prev_ot
        FROM {table_name}
        WHERE open_time >= {window_start_ms} AND open_time <= {window_end_ms}
        ORDER BY open_time
        """
    )

    gaps: List[Tuple[int, int]] = []
    for row in rs.result_rows:
        ot = int(row[0])
        prev = int(row[1] or 0)
        if prev < min_ot:
            continue
        if ot - prev > step_ms:
            gaps.append((prev + step_ms, ot - step_ms))

    # 头缺口
    if min_ot > window_start_ms:
        gaps.append((window_start_ms, min_ot - step_ms))

    # 尾缺口
    last_expected = max_ot + step_ms
    if last_expected <= window_end_ms:
        gaps.append((last_expected, window_end_ms))

    gaps.sort(key=lambda g: g[0])
    return gaps

def insert_indicator_ma_incremental(
    client: Client,
    source_table: str,
    symbol: str,
    market: str,
    period: str,
    start_ms: int,
    end_ms: int,
) -> None:
    client.query(
        f"""
        INSERT INTO indicator_ma (symbol, market, period, open_time, ma20, ma50)
        SELECT
          %(symbol)s AS symbol,
          %(market)s AS market,
          %(period)s AS period,
          open_time,
          avg(toFloat64(close)) OVER (ORDER BY open_time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
          avg(toFloat64(close)) OVER (ORDER BY open_time ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50
        FROM {source_table}
        WHERE open_time >= %(s)s AND open_time <= %(e)s
        """,
        parameters={
            "symbol": symbol,
            "market": market,
            "period": period,
            "s": start_ms,
            "e": end_ms,
        },
    )

def get_latest_enabled_proxy(client: Client) -> Tuple[str, int, str | None, str | None] | None:
    rs = client.query(
        "SELECT host, port, username, password FROM proxy_config WHERE enabled = 1 ORDER BY updated_at DESC LIMIT 1"
    )
    if not rs.result_rows:
        return None
    host, port, username, password = rs.result_rows[0]
    return str(host), int(port), (username or None), (password or None)