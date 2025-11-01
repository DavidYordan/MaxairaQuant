from __future__ import annotations
import time
from ...db.client import AsyncClickHouseClient, get_client_manager
from typing import Optional, Tuple
from loguru import logger


class ProxyRegistry:
    """代理配置注册表 - 带缓存和独立连接"""
    def __init__(self):
        self._cache: Optional[Tuple[str, int, str, str, float]] = None  # (host, port, username, password, cache_time)
        self._cache_ttl = 60.0  # 缓存60秒
    
    async def get_enabled_proxy_url(self) -> Optional[str]:
        """获取启用的代理URL，带缓存机制"""
        try:
            now = time.time()
            if self._cache and (now - self._cache[4]) < self._cache_ttl:
                host, port, username, password = self._cache[:4]
                return self._build_proxy_url(host, port, username, password)

            # 按需拉取 read client
            read_client = await get_client_manager().get_read()
            rec = await self._get_latest_enabled_proxy_safe(read_client)
            if rec is None:
                self._cache = None
                return None
            host, port, username, password = rec
            self._cache = (host, port, username, password, now)
            return self._build_proxy_url(host, port, username, password)
        except Exception as e:
            logger.error(f"获取代理URL失败: {e}")
            return None
    
    def _build_proxy_url(self, host: str, port: int, username: Optional[str], password: Optional[str]) -> str:
        """构建代理URL"""
        auth = f"{username}:{password}" if username and password else ""
        scheme = "socks5"
        if auth:
            return f"{scheme}://{auth}@{host}:{int(port)}"
        return f"{scheme}://{host}:{int(port)}"
    
    async def _get_latest_enabled_proxy_safe(self, client: AsyncClickHouseClient) -> Tuple[str, int, str | None, str | None] | None:
        """安全的代理配置查询，避免并发冲突"""
        try:
            rs = await client.query(
                """SELECT host, port, username, password 
                FROM proxy_config 
                WHERE enabled = 1 
                ORDER BY updated_at DESC 
                LIMIT 1"""
            )
            if not rs.result_rows:
                return None
            host, port, username, password = rs.result_rows[0]
            return str(host), int(port), (username or None), (password or None)
        except Exception as e:
            logger.error(f"获取代理配置失败: {e}")
            return None
    
    def invalidate_cache(self):
        """使缓存失效"""
        self._cache = None


# 全局代理注册表实例
_proxy_registry = ProxyRegistry()


async def get_enabled_proxy_url() -> Optional[str]:
    """获取启用的代理URL - 兼容性接口"""
    return await _proxy_registry.get_enabled_proxy_url()


def invalidate_proxy_cache():
    """使代理缓存失效 - 用于配置更新时"""
    _proxy_registry.invalidate_cache()