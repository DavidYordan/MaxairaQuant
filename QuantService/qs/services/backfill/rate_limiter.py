from __future__ import annotations
import asyncio
import time

class ApiRateLimiter:
    def __init__(self, rps: int, minute_block_threshold: int = 1150):
        self._rps = rps
        self._tokens = asyncio.Queue(maxsize=rps)
        self._refill_task = None
        self._used_weight_minute: int = 0
        self._minute_block_threshold = minute_block_threshold

    async def start(self):
        await self._fill()
        if self._refill_task is None:
            self._refill_task = asyncio.create_task(self._refill_loop())

    async def stop(self):
        if self._refill_task:
            self._refill_task.cancel()
            try:
                await self._refill_task
            except Exception:
                pass
            self._refill_task = None

    async def acquire(self):
        await self._tokens.get()

    def release(self):
        # 令牌桶语义下无需释放；保持兼容占位
        pass

    def update_from_headers(self, headers: dict):
        try:
            v = headers.get("X-MBX-USED-WEIGHT-1M") or headers.get("x-mbx-used-weight-1m")
            if v is not None:
                self._used_weight_minute = int(v)
        except Exception:
            pass

    async def maybe_block_by_minute_weight(self):
        if self._used_weight_minute >= self._minute_block_threshold:
            now = time.time()
            sleep_s = (int(now // 60) * 60 + 60) - now
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)

    async def _fill(self):
        try:
            while True:
                self._tokens.get_nowait()
        except asyncio.QueueEmpty:
            pass
        for _ in range(self._rps):
            await self._tokens.put(None)

    async def _refill_loop(self):
        try:
            while True:
                await asyncio.sleep(1.0)
                await self._fill()
        except asyncio.CancelledError:
            pass