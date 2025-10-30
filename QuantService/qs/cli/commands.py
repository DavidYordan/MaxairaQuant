import argparse
import asyncio
from config.loader import load_config
from db.client import get_client
from services.backfill.manager import BackfillManager
from services.ws.supervisor import WebSocketSupervisor
from services.ws.client_server import ClientServer
from services.ws.event_bus import EventBus
from common.types import MarketType

def main():
    parser = argparse.ArgumentParser(prog="qs-cli", description="QuantService CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # 启动客户端下发服务（独立运行）
    p_cs = sub.add_parser("clientserver", help="Start client downlink server")
    p_cs.add_argument("--host", default="0.0.0.0")
    p_cs.add_argument("--port", type=int, default=8765)
    p_cs.add_argument("--qps", type=int, default=20)

    # 手工触发一次回填缺口
    p_bf = sub.add_parser("backfill-gap", help="Backfill a gap window")
    p_bf.add_argument("--symbol", required=True)
    p_bf.add_argument("--market", choices=["spot", "um", "cm"], required=True)
    p_bf.add_argument("--period", choices=["1m", "1h"], required=True)
    p_bf.add_argument("--start_ms", type=int, required=True)
    p_bf.add_argument("--end_ms", type=int, required=True)

    p_ws = sub.add_parser("ws-start", help="Start a WS upstream (temporary)")
    p_ws.add_argument("--symbol", required=True)
    p_ws.add_argument("--market", choices=["spot", "um", "cm"], required=True)
    p_ws.add_argument("--period", choices=["1m", "1h"], required=True)

    args = parser.parse_args()
    cfg = load_config("config/app.yaml")
    client = get_client(cfg.clickhouse)

    async def run():
        bus = EventBus()
        if args.cmd == "clientserver":
            srv = ClientServer(client, host=args.host, port=args.port, qps=args.qps, event_bus=bus)
            await srv.start()
            try:
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                await srv.stop()
        elif args.cmd == "backfill-gap":
            mgr = BackfillManager(cfg, client, event_bus=bus)
            await mgr.start()
            await mgr.backfill_gap(MarketType(args.market), args.symbol, args.period, args.start_ms, args.end_ms)
            await mgr.stop()
        elif args.cmd == "ws-start":
            sup = WebSocketSupervisor(cfg, client, event_bus=bus)
            await sup.start_stream(args.market, args.symbol, args.period)
            try:
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                await sup.stop_stream(args.market, args.symbol, args.period)

    asyncio.run(run())

if __name__ == "__main__":
    main()