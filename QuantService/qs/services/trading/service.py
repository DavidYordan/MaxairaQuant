from __future__ import annotations
import asyncio
from typing import Dict, Any, Optional
import httpx
from loguru import logger

class OrderGateway:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

    async def send_order(self, payload: Dict[str, Any], idempotency_key: str) -> Dict[str, Any]:
        headers = {"X-API-KEY": self.api_key, "X-IDEMPOTENCY-KEY": idempotency_key}
        timeout = httpx.Timeout(10.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(f"{self.base_url}/order", json=payload, headers=headers)
            r.raise_for_status()
            return r.json()

class TradingService:
    def __init__(self, gateway: OrderGateway):
        self.gateway = gateway

    async def place_order_with_risk(self, payload: Dict[str, Any], risk_cfg: Dict[str, Any], idempotency_key: str) -> Optional[Dict[str, Any]]:
        if not self._risk_check(payload, risk_cfg):
            logger.warning("风控拒绝下单：{}", payload)
            return None
        backoff = 0.5
        for attempt in range(5):
            try:
                resp = await self.gateway.send_order(payload, idempotency_key)
                return resp
            except Exception as e:
                logger.error("下单失败（{}）：{}，重试中", attempt + 1, str(e))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
        self._alert(f"下单失败，超过重试次数：{payload}")
        return None

    def _risk_check(self, payload: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
        # 示例：名义价值与数量限制
        qty = float(payload.get("quantity", 0.0))
        price = float(payload.get("price", 0.0))
        notional = qty * price
        if notional > float(cfg.get("max_notional", 0.0)):
            return False
        return True

    def _alert(self, msg: str) -> None:
        # 占位：可接 Slack/邮件/短信
        logger.error("ALERT: {}", msg)