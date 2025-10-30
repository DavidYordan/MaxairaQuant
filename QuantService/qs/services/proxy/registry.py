from __future__ import annotations
from clickhouse_connect.driver.client import Client
from typing import Optional
from ...db.queries import get_latest_enabled_proxy

def get_enabled_proxy_url(client: Client) -> Optional[str]:
    rec = get_latest_enabled_proxy(client)
    if rec is None:
        return None
    host, port, username, password = rec
    auth = f"{username}:{password}" if username and password else ""
    scheme = "socks5"
    if auth:
        return f"{scheme}://{auth}@{host}:{int(port)}"
    return f"{scheme}://{host}:{int(port)}"