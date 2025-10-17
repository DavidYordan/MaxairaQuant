from __future__ import annotations
import asyncio
from qs.bootstrap.daemon import run_daemon

async def main():
    await run_daemon()

if __name__ == "__main__":
    asyncio.run(main())