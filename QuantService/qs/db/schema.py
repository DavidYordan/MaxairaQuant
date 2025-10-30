from __future__ import annotations
from clickhouse_connect.driver.client import Client

def ensure_base_tables(client: Client) -> None:
    client.command(
        """
        CREATE TABLE IF NOT EXISTS trading_pair_config
        (
          id UInt64,
          symbol String,
          market_type String,
          enabled UInt8,
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = MergeTree
        ORDER BY (symbol, market_type)
        """
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS proxy_config
        (
          id UInt64,
          host String,
          port UInt16,
          username String,
          password String,
          enabled UInt8,
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = MergeTree
        ORDER BY (host, port)
        """
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS api_keys
        (
          api_key String,
          enabled UInt8,
          qps_limit UInt32,
          created_at DateTime DEFAULT now(),
          updated_at DateTime DEFAULT now()
        ) ENGINE = MergeTree
        ORDER BY (api_key)
        """
    )

def kline_table_name(symbol: str, market_type: str, period: str) -> str:
    safe_symbol = symbol.lower().replace("-", "_").replace("/", "_")
    safe_market = market_type.lower()
    safe_period = period.lower()
    return f"kline_{safe_symbol}_{safe_market}_{safe_period}"

def ensure_kline_table(client: Client, table_name: str) -> None:
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
          open_time UInt64,
          close_time UInt64,
          open Decimal(38,8),
          high Decimal(38,8),
          low Decimal(38,8),
          close Decimal(38,8),
          volume Decimal(38,8),
          quote_volume Decimal(38,8),
          count UInt64,
          taker_buy_base_volume Decimal(38,8),
          taker_buy_quote_volume Decimal(38,8),
          version UInt64 DEFAULT toUInt64(toUnixTimestamp(now()))
        ) ENGINE = ReplacingMergeTree(version)
        PARTITION BY toYYYYMM(toDateTime(open_time/1000))
        ORDER BY open_time
        """
    )

def ensure_trading_pair_entry(client: Client, symbol: str, market_type: str, enabled: int) -> None:
    rs = client.query(
        "SELECT count() FROM trading_pair_config WHERE symbol = %(s)s AND market_type = %(m)s",
        parameters={"s": symbol, "m": market_type},
    )
    cnt = int(rs.result_rows[0][0]) if rs.result_rows else 0
    if cnt == 0:
        rs2 = client.query("SELECT coalesce(max(id), 0) FROM trading_pair_config")
        max_id = int(rs2.result_rows[0][0]) if rs2.result_rows else 0
        new_id = max_id + 1
        client.insert(
            "trading_pair_config",
            [[new_id, symbol, market_type, int(enabled)]],
            column_names=["id", "symbol", "market_type", "enabled"],
        )

def ensure_bootstrap_defaults(client: Client, assets: list[str]) -> None:
    markets = ["spot", "um", "cm"]
    periods = ["1m", "1h"]
    for asset in assets:
        base = asset.lower()
        symbol = f"{base}usdt".upper()
        for m in markets:
            for p in periods:
                tbl = kline_table_name(symbol, m, p)
                ensure_kline_table(client, tbl)
            # 仅 ETHUSDT@um 默认启用，其它默认禁用
            default_enabled = 1 if (symbol == "ETHUSDT" and m == "um") else 0
            ensure_trading_pair_entry(client, symbol, m, default_enabled)

def ensure_indicator_tables(client: Client) -> None:
    client.command(
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

def indicator_view_name(symbol: str, market: str, period: str) -> str:
    return f"mv_indicator_ma_{symbol.lower()}_{market.lower()}_{period.lower()}"

def ensure_indicator_view_for_table(client: Client, symbol: str, market: str, period: str) -> None:
    table = kline_table_name(symbol, market, period)
    view_name = indicator_view_name(symbol, market, period)
    client.command(
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

def ensure_indicator_views_for_pairs(client: Client, pairs: list[tuple[str, str]], periods: list[str]) -> None:
    for symbol, market in pairs:
        for period in periods:
            ensure_indicator_view_for_table(client, symbol, market, period)

def ensure_backtest_tables(client: Client) -> None:
    client.command(
        """
        CREATE TABLE IF NOT EXISTS backtest_results
        (
          job_id String,
          symbol String,
          market String,
          period String,
          params_json String,
          metric_pnl Float64,
          metric_sharpe Float64,
          trades_json String,
          started_at DateTime DEFAULT now(),
          finished_at DateTime DEFAULT now()
        ) ENGINE = MergeTree
        ORDER BY (job_id, symbol, market, period)
        """
    )