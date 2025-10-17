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
    # 新增 API Key 管理表（用于 ClientServer 鉴权与连接级限速）
    client.command(
        """
        CREATE TABLE IF NOT EXISTS api_keys
        (
          id UInt64,
          api_key String,
          name String,
          qps_limit UInt16 DEFAULT 20,
          enabled UInt8,
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