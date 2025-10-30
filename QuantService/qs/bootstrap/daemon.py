import asyncio
from pathlib import Path
from loguru import logger
from ..config.loader import load_config
from ..db.client import get_client
from ..db.schema import ensure_base_tables, ensure_kline_table, kline_table_name, ensure_default_trading_pair
from ..db.queries import get_enabled_pairs
from ..services.backfill.manager import BackfillManager
from ..scheduler.gap_heal import GapHealScheduler
from ..services.ws.supervisor import WebSocketSupervisor
from ..services.ws.client_server import ClientServer
from ..services.indicator.offline import IndicatorOfflineService
from ..services.indicator.online import IndicatorOnlineService
from ..services.ws.event_bus import EventBus

async def run_daemon(cfg_path: Path | str = None):
    # 1) 配置与DB
    cfg_path = cfg_path or (Path(__file__).parents[2] / "config" / "app.yaml")
    cfg = load_config(cfg_path)
    client = get_client(cfg.clickhouse)
    ensure_base_tables(client)

    # 插入默认交易对并预创建 K线表（1m/1h）
    ensure_default_trading_pair(client, symbol="ethusdt", market_type="um")
    for period in ["1m", "1h"]:
        tbl = kline_table_name("ethusdt", "um", period)
        ensure_kline_table(client, tbl)

    # 2) 指标离线初始化
    ind_off = IndicatorOfflineService(client)
    ind_off.ensure_tables()
    pairs = get_enabled_pairs(client)
    ind_off.bootstrap_materialized_views(pairs, ["1m", "1h"])

    # 3) 组装各服务
    event_bus = EventBus()
    backfill = BackfillManager(cfg, client, event_bus=event_bus)
    scheduler = GapHealScheduler(cfg, client, backfill)
    ws_sup = WebSocketSupervisor(cfg, client, event_bus=event_bus)
    ind_on = IndicatorOnlineService(client)
    client_srv = ClientServer(client, event_bus=event_bus)

    # 4) 启动顺序
    await backfill.start()
    await scheduler.start()
    await ws_sup.start_enabled_streams()
    await ind_on.start(pairs, ["1m", "1h"])
    await client_srv.start()
    logger.info("守护已启动：回填/缺口调度/WS/指标在线/ClientServer")

    # 5) 指标日志
    async def _metrics_loop():
        while True:
            snap = backfill.metrics.snapshot()
            logger.info("Metrics: req={} ok={} fail={} rows={}",
                        snap.requests, snap.successes, snap.failures, snap.inserted_rows)
            await asyncio.sleep(60)
    asyncio.create_task(_metrics_loop())

    # 6) 常驻
    try:
        while True:
            await asyncio.sleep(5)
    finally:
        await client_srv.stop()
        await ind_on.stop()
        await scheduler.stop()
        await backfill.stop()