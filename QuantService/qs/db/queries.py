from __future__ import annotations
from typing import List, Tuple
from clickhouse_connect.driver.client import Client
from common.types import Kline


def get_enabled_pairs(client: Client) -> List[Tuple[str, str]]:
    rs = client.query(
        "SELECT symbol, market_type FROM trading_pair_config WHERE enabled = 1 ORDER BY symbol, market_type"
    )
    return [(row[0], row[1]) for row in rs.result_rows]


def get_max_open_time(client: Client, table_name: str) -> int:
    rs = client.query(f"SELECT max(open_time) FROM {table_name}")
    val = rs.first_row[0]
    return int(val or 0)


def insert_klines(client: Client, table_name: str, klines: List[Kline]) -> None:
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
    client.insert(
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
        f"SELECT count() AS c, min(open_time) AS min_ot, max(open_time) AS max_ot FROM {table_name} WHERE open_time >= %(s)s AND open_time <= %(e)s",
        params={"s": window_start_ms, "e": window_end_ms},
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
        WHERE open_time >= %(s)s AND open_time <= %(e)s
        ORDER BY open_time
        """,
        params={"s": window_start_ms, "e": window_end_ms},
    )
    gaps: List[Tuple[int, int]] = []
    for row in rs.result_rows:
        ot = int(row[0])
        prev = row[1]
        if prev is None:
            continue
        prev = int(prev)
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