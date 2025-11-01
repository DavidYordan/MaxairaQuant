from __future__ import annotations
from loguru import logger
from typing import List, Tuple
from ..common.types import Kline
from .client import get_client_manager


async def get_enabled_pairs() -> List[Tuple[str, str]]:
    """获取启用的交易对"""
    client = await get_client_manager().get_read()
    rs = await client.query(
        "SELECT symbol, market_type FROM trading_pair_config WHERE enabled = 1 ORDER BY symbol, market_type"
    )
    return [(str(row[0]), str(row[1])) for row in rs.result_rows]


async def get_max_open_time(table_name: str) -> int:
    """获取表中最大的开盘时间"""
    client = await get_client_manager().get_read()
    rs = await client.query(f"SELECT max(open_time) FROM {table_name}")
    val = rs.first_row[0]
    return int(val or 0)


async def insert_klines(table_name: str, klines: List[Kline]) -> None:
    if not klines:
        return
    data = [
        [
            k.open_time_ms,
            k.close_time_ms,
            str(k.open),
            str(k.high),
            str(k.low),
            str(k.close),
            str(k.volume),
            str(k.quote_volume),
            k.count,
            str(k.taker_buy_base_volume),
            str(k.taker_buy_quote_volume),
        ]
        for k in klines
    ]
    column_names = [
        "open_time", "close_time", "open", "high", "low", "close",
        "volume", "quote_volume", "count", "taker_buy_base_volume", "taker_buy_quote_volume"
    ]
    client = await get_client_manager().get_write()
    await client.insert(table_name, data, column_names=column_names)


async def insert_klines_bulk(table_name: str, klines: List[Kline]) -> None:
    if not klines:
        return
    data = [
        [
            k.open_time_ms,
            k.close_time_ms,
            str(k.open),
            str(k.high),
            str(k.low),
            str(k.close),
            str(k.volume),
            str(k.quote_volume),
            k.count,
            str(k.taker_buy_base_volume),
            str(k.taker_buy_quote_volume),
        ]
        for k in klines
    ]
    column_names = [
        "open_time", "close_time", "open", "high", "low", "close",
        "volume", "quote_volume", "count", "taker_buy_base_volume", "taker_buy_quote_volume"
    ]
    client = await get_client_manager().get_bulkwrite()
    await client.insert(table_name, data, column_names=column_names)


async def find_gaps_windowed_sql(
    table_name: str,
    window_start_ms: int,
    window_end_ms: int,
    step_ms: int,
) -> List[Tuple[int, int]]:
    """使用窗口化SQL查找数据缺口（bulkread）"""
    client = await get_client_manager().get_bulkread()
    try:
        agg = await client.query(
            f"""SELECT 
                count() AS c, 
                min(open_time) AS min_ot, 
                max(open_time) AS max_ot 
            FROM {table_name} 
            WHERE open_time >= {window_start_ms} AND open_time <= {window_end_ms}"""
        )
        count = int(agg.first_row[0] or 0)
        if count == 0:
            return [(window_start_ms, window_end_ms)]
        min_ot = int(agg.first_row[1])
        max_ot = int(agg.first_row[2])

        rs = await client.query(
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

        if min_ot > window_start_ms:
            gaps.append((window_start_ms, min_ot - step_ms))
        last_expected = max_ot + step_ms
        if last_expected <= window_end_ms:
            gaps.append((last_expected, window_end_ms))
        gaps.sort(key=lambda g: g[0])
        return gaps
    except Exception as e:
        logger.error(f"查找缺口失败 {table_name}: {e}")
        raise

async def insert_indicator_ma_incremental(
    source_table: str,
    symbol: str,
    market: str,
    period: str,
    start_ms: int,
    end_ms: int,
) -> None:
    """指标增量写入（使用 write）"""
    client = await get_client_manager().get_write()
    await client.query(
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