from __future__ import annotations
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from config.schema import ClickHouseConfig


def get_client(cfg: ClickHouseConfig) -> Client:
    client = clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
        send_receive_timeout=cfg.timeout_ms // 1000,
    )
    if cfg.async_insert:
        client.command("SET async_insert = 1")
        client.command("SET wait_for_async_insert = 1")
    return client