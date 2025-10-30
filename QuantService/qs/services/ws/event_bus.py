from __future__ import annotations
import asyncio
from typing import Dict, List, Any

class EventBus:
    def __init__(self):
        self._subs: Dict[str, List[asyncio.Queue]] = {}

    async def subscribe(self, topic: str, maxsize: int = 100) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=maxsize)
        self._subs.setdefault(topic, []).append(q)
        return q

    def unsubscribe(self, topic: str, q: asyncio.Queue) -> None:
        lst = self._subs.get(topic)
        if not lst:
            return
        try:
            lst.remove(q)
        except ValueError:
            pass
        if not lst:
            self._subs.pop(topic, None)

    async def publish(self, topic: str, payload: Any) -> None:
        lst = self._subs.get(topic)
        if not lst:
            return
        for q in list(lst):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    _ = q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    pass