from __future__ import annotations
import asyncio
from ...db.client import AsyncClickHouseClient
from typing import Optional
from ...db.queries import get_latest_enabled_proxy

async def get_enabled_proxy_url(client: AsyncClickHouseClient) -> Optional[str]:
    rec = await get_latest_enabled_proxy(client)
    if rec is None:
        return None
    host, port, username, password = rec
    auth = f"{username}:{password}" if username and password else ""
    scheme = "socks5"
    if auth:
        return f"{scheme}://{auth}@{host}:{int(port)}"
    return f"{scheme}://{host}:{int(port)}"