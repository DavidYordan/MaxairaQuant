from __future__ import annotations
from clickhouse_connect.driver.client import Client
from typing import Optional

def get_enabled_proxy_url(client: Client) -> Optional[str]:
    rs = client.query(
        "SELECT host, port, username, password FROM proxy_config WHERE enabled = 1 ORDER BY updated_at DESC LIMIT 1"
    )
    if not rs.result_rows:
        return None
    host, port, username, password = rs.result_rows[0]
    auth = f"{username}:{password}" if username and password else ""
    scheme = "socks5"
    if auth:
        return f"{scheme}://{auth}@{host}:{int(port)}"
    return f"{scheme}://{host}:{int(port)}"