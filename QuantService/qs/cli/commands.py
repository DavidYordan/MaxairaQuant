import argparse
import asyncio
from config.loader import load_config
from db.client import get_clients
from loguru import logger
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

    async def run():
        """主运行函数 - 改进的异步生命周期管理"""
        clients = None
        services = []
        
        try:
            # 初始化数据库客户端（三个池）
            clients = await get_clients(cfg.clickhouse, pool_read=8, pool_write=8, pool_backfill=16)
            bus = EventBus()
            
            if args.cmd == "clientserver":
                srv = ClientServer(clients.read, host=args.host, port=args.port, qps=args.qps, event_bus=bus)
                services.append(srv)
                await srv.start()
                
                logger.info(f"ClientServer 已启动在 {args.host}:{args.port}")
                
                # 优雅等待中断信号
                shutdown_event = asyncio.Event()
                
                def signal_handler():
                    logger.info("收到停止信号")
                    shutdown_event.set()
                
                # 注册信号处理器（如果在主线程中）
                try:
                    import signal
                    signal.signal(signal.SIGINT, lambda s, f: signal_handler())
                    signal.signal(signal.SIGTERM, lambda s, f: signal_handler())
                except ValueError:
                    # 不在主线程中，使用KeyboardInterrupt
                    pass
                
                try:
                    await shutdown_event.wait()
                except KeyboardInterrupt:
                    logger.info("收到键盘中断")
                
            elif args.cmd == "backfill-gap":
                mgr = BackfillManager(cfg, clients.read, clients.backfill)
                services.append(mgr)
                await mgr.start()
                await mgr.backfill_gap(
                    MarketType(args.market), 
                    args.symbol, 
                    args.period, 
                    args.start_ms, 
                    args.end_ms
                )
            elif args.cmd == "ws-start":
                sup = WebSocketSupervisor(cfg, clients.read, clients.write, event_bus=bus)
                services.append(sup)
                await sup.start_stream(args.market, args.symbol, args.period)
                logger.info(f"WebSocket流已启动: {args.market} {args.symbol} {args.period}")
                
                # 等待中断信号
                try:
                    while True:
                        await asyncio.sleep(1)
                except KeyboardInterrupt:
                    logger.info("收到停止信号")
                    
        except Exception as e:
            logger.error(f"运行时错误: {e}")
            raise
        finally:
            # 优雅关闭所有服务
            logger.info("正在关闭服务...")
            
            shutdown_tasks = []
            for service in reversed(services):  # 反向关闭
                if hasattr(service, 'stop'):
                    task = asyncio.create_task(
                        service.stop(),
                        name=f"stop_{service.__class__.__name__}"
                    )
                    shutdown_tasks.append(task)
            
            if shutdown_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*shutdown_tasks, return_exceptions=True),
                        timeout=30.0
                    )
                except asyncio.TimeoutError:
                    logger.warning("部分服务关闭超时")
            
            # 关闭数据库客户端
            if clients:
                try:
                    await clients.read.close()
                    await clients.write.close()
                    await clients.backfill.close()
                    logger.info("数据库连接已关闭")
                except Exception as e:
                    logger.error(f"关闭数据库连接失败: {e}")
            logger.info("所有服务已关闭")

    asyncio.run(run())

if __name__ == "__main__":
    main()