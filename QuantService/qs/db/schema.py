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
    # 命名遵循 service_bak_core.md：kline_{symbol}_{marketType}_{period}
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

def ensure_default_trading_pair(client: Client, symbol: str = "ethusdt", market_type: str = "um") -> None:
    rs = client.query(
        "SELECT count() FROM trading_pair_config WHERE symbol = %(s)s AND market_type = %(m)s",
        params={"s": symbol, "m": market_type},
    )
    cnt = int(rs.result_rows[0][0]) if rs.result_rows else 0
    if cnt == 0:
        rs2 = client.query("SELECT coalesce(max(id), 0) FROM trading_pair_config")
        max_id = int(rs2.result_rows[0][0]) if rs2.result_rows else 0
        new_id = max_id + 1
        client.command(
            f"INSERT INTO trading_pair_config (id, symbol, market_type, enabled) "
            f"VALUES ({new_id}, '{symbol}', '{market_type}', 1)"
        )