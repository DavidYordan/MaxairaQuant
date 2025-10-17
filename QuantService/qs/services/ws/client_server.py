from __future__ import annotations
import asyncio
import json
import gzip
import time
from typing import Optional, Dict, Any, List, Tuple
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
from loguru import logger
from db.schema import kline_table_name
from db.client import Client

class QueryLimiter:
    def __init__(self, qps: int):
        self._qps = qps
        self._tokens = asyncio.Queue(maxsize=qps)
        self._refill_task: Optional[asyncio.Task] = None

    async def start(self):
        await self._fill()
        if self._refill_task is None:
            self._refill_task = asyncio.create_task(self._refill_loop())

    async def stop(self):
        if self._refill_task:
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass
            self._refill_task = None

    async def acquire(self):
        await self._tokens.get()

    async def _fill(self):
        try:
            while True:
                self._tokens.get_nowait()
        except asyncio.QueueEmpty:
            pass
        for _ in range(self._qps):
            await self._tokens.put(None)

    async def _refill_loop(self):
        try:
            while True:
                await asyncio.sleep(1.0)
                await self._fill()
        except asyncio.CancelledError:
            pass

class HotCache:
    def __init__(self, last_hours: int = 6, ttl_s: float = 5.0):
        self._ttl_s = ttl_s
        self._window_ms = last_hours * 3600 * 1000
        self._data: Dict[Tuple[str, str, str, int, int, int], Tuple[float, Any]] = {}

    def get(self, key: Tuple[str, str, str, int, int, int]) -> Optional[Any]:
        ts_data = self._data.get(key)
        if not ts_data:
            return None
        ts, payload = ts_data
        if time.time() - ts > self._ttl_s:
            return None
        return payload

    def set(self, key: Tuple[str, str, str, int, int, int], payload: Any):
        self._data[key] = (time.time(), payload)

    def in_hot_window(self, end_ms: int) -> bool:
        now_ms = int(time.time() * 1000)
        return end_ms >= now_ms - self._window_ms

class ClientServer:
    def __init__(self, ch_client: Client, host: str = "0.0.0.0", port: int = 8765, qps: int = 20, hot_hours: int = 6, cache_ttl_s: float = 5.0, auth_required: bool = True, default_qps: int = 20, max_page_size: int = 2000, max_subscriptions: int = 2, min_poll_interval_ms: int = 500):
        self.client = ch_client
        self.host = host
        self.port = port
        self._server: Optional[websockets.server.Serve] = None
        self._limiter = QueryLimiter(qps)
        self._cache = HotCache(last_hours=hot_hours, ttl_s=cache_ttl_s)
        # 安全与公平性配置
        self.auth_required = auth_required
        self.default_qps = default_qps
        self.max_page_size = max_page_size
        self.max_subscriptions = max_subscriptions
        self.min_poll_interval_ms = min_poll_interval_ms

    async def start(self):
        await self._limiter.start()
        self._server = await websockets.serve(self._handler, self.host, self.port, ping_interval=20, ping_timeout=20, max_queue=1024)
        logger.info("ClientServer 启动：ws://{}:{}/", self.host, self.port)

    async def stop(self):
        await self._limiter.stop()
        if self._server:
            self._server.ws_server.close()
            await self._server.ws_server.wait_closed()
            self._server = None
        logger.info("ClientServer 已停止")

    async def _handler(self, ws: websockets.WebSocketServerProtocol):
        from dataclasses import dataclass
        @dataclass
        class ConnContext:
            api_key: Optional[str] = None
            limiter: Optional[QueryLimiter] = None
            subs_count: int = 0

        conn = ConnContext()
        try:
            async for raw in ws:
                try:
                    req = self._parse_incoming(raw)
                    action = req.get("action")
                    compression = req.get("compression")
                    # 鉴权
                    if self.auth_required and conn.api_key is None:
                        if action != "auth":
                            await self._send(ws, {"error": "unauthorized", "hint": "send {action:'auth', api_key:''} first"}, compression)
                            continue
                        api_key = req.get("api_key")
                        ok, qps_limit = await self._validate_api_key(api_key or "")
                        if not ok:
                            await self._send(ws, {"error": "invalid_api_key"}, compression)
                            await ws.close()
                            break
                        conn.api_key = api_key
                        conn.limiter = QueryLimiter(qps_limit or self.default_qps)
                        await conn.limiter.start()
                        await self._send(ws, {"ok": True, "qps_limit": qps_limit}, compression)
                        continue

                    # 全局与连接级限速
                    await self._limiter.acquire()
                    if conn.limiter:
                        await conn.limiter.acquire()

                    if action == "query":
                        resp = await self._handle_query(req)
                        await self._send(ws, resp, compression)
                    elif action == "subscribe_incremental":
                        if conn.subs_count >= self.max_subscriptions:
                            await self._send(ws, {"error": "subscription_limit", "max": self.max_subscriptions}, compression)
                            continue
                        conn.subs_count += 1
                        try:
                            await self._handle_subscribe_incremental(ws, req, conn)
                        finally:
                            conn.subs_count -= 1
                    else:
                        await self._send(ws, {"error": "unknown action"}, compression)
                except Exception as e:
                    await self._send(ws, {"error": f"bad_request: {str(e)}"}, None)
        except (ConnectionClosedOK, ConnectionClosedError):
            pass
        except Exception as e:
            logger.warning("ClientServer 连接异常: {}", str(e))
        finally:
            # 释放连接级限速器
            if conn.limiter:
                await conn.limiter.stop()

    def _parse_incoming(self, raw: Any) -> Dict[str, Any]:
        if isinstance(raw, bytes):
            # 兼容客户端发送的二进制帧（假设未压缩）
            text = raw.decode("utf-8", errors="ignore")
        else:
            text = raw
        return json.loads(text)

    async def _send(self, ws: websockets.WebSocketServerProtocol, payload: Any, compression: Optional[str]):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if compression == "gzip":
            data = gzip.compress(data)
            await ws.send(data)  # 二进制帧
        else:
            await ws.send(data.decode("utf-8"))

    async def _handle_query(self, req: Dict[str, Any]) -> Dict[str, Any]:
        symbol = req["symbol"]
        market = req["market"]
        period = req["period"]
        start_ms = int(req.get("start_open_time_ms", 0))
        end_ms = int(req.get("end_open_time_ms", 2**63 - 1))
        limit = int(req.get("page_size", 1000))
        # 连接级配额：限制每页大小
        limit = min(limit, self.max_page_size)
        table = kline_table_name(symbol, market, period)
        key = (symbol.lower(), market.lower(), period.lower(), start_ms, end_ms, limit)

        if self._cache.in_hot_window(end_ms):
            cached = self._cache.get(key)
            if cached is not None:
                return {"ok": True, "cached": True, "rows": cached, "next_open_time_ms": cached[-1]["open_time"] + 1 if cached else start_ms}

        rows = await self._fetch_page(table, start_ms, end_ms, limit)
        self._cache.set(key, rows)
        next_cursor = rows[-1]["open_time"] + 1 if rows else start_ms
        return {"ok": True, "cached": False, "rows": rows, "next_open_time_ms": next_cursor}

    async def _handle_subscribe_incremental(self, ws: websockets.WebSocketServerProtocol, req: Dict[str, Any], conn_ctx=None):
        symbol = req["symbol"]
        market = req["market"]
        period = req["period"]
        cursor_ms = int(req.get("from_open_time_ms", 0))
        page_size = int(req.get("page_size", 1000))
        compression = req.get("compression")
        interval_ms = int(req.get("poll_interval_ms", 2000))
        # 限制订阅轮询频率与每页大小
        interval_ms = max(interval_ms, self.min_poll_interval_ms)
        page_size = min(page_size, self.max_page_size)
        table = kline_table_name(symbol, market, period)

        try:
            while True:
                # 每次循环也遵守连接/全局限速
                await self._limiter.acquire()
                if conn_ctx and conn_ctx.limiter:
                    await conn_ctx.limiter.acquire()

                rows = await self._fetch_page(table, cursor_ms, 2**63 - 1, page_size)
                if rows:
                    cursor_ms = rows[-1]["open_time"] + 1
                    await self._send(ws, {"ok": True, "rows": rows, "next_open_time_ms": cursor_ms}, compression)
                await asyncio.sleep(interval_ms / 1000.0)
        except (ConnectionClosedOK, ConnectionClosedError):
            pass

    async def _fetch_page(self, table: str, start_ms: int, end_ms: int, limit: int) -> List[Dict[str, Any]]:
        rs = await asyncio.to_thread(
            self.client.query,
            f"""
            SELECT
              open_time, close_time, toString(open), toString(high), toString(low), toString(close),
              toString(volume), toString(quote_volume), count, toString(taker_buy_base_volume), toString(taker_buy_quote_volume)
            FROM {table}
            WHERE open_time >= %(s)s AND open_time <= %(e)s
            ORDER BY open_time
            LIMIT %(l)s
            """,
            params={"s": start_ms, "e": end_ms, "l": limit},
        )
        rows: List[Dict[str, Any]] = []
        for r in rs.result_rows:
            rows.append({
                "open_time": int(r[0]),
                "close_time": int(r[1]),
                "open": r[2],
                "high": r[3],
                "low": r[4],
                "close": r[5],
                "volume": r[6],
                "quote_volume": r[7],
                "count": int(r[8]),
                "taker_buy_base_volume": r[9],
                "taker_buy_quote_volume": r[10],
            })
        return rows

    async def _validate_api_key(self, api_key: str) -> Tuple[bool, int]:
        try:
            rs = await asyncio.to_thread(
                self.client.query,
                "SELECT enabled, qps_limit FROM api_keys WHERE api_key = %(k)s LIMIT 1",
                params={"k": api_key},
            )
            if not rs.result_rows:
                return False, 0
            enabled, qps_limit = rs.result_rows[0]
            return (int(enabled) == 1), int(qps_limit or self.default_qps)
        except Exception:
            return False, 0