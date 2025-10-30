import asyncio
from pathlib import Path
from loguru import logger
from ..config.loader import load_config
from ..db.client import get_client
from ..db.schema import ensure_base_tables, ensure_bootstrap_defaults, ensure_indicator_tables, ensure_backtest_tables, ensure_indicator_views_for_pairs
from ..db.queries import get_enabled_pairs
from ..services.backfill.manager import BackfillManager
from ..scheduler.gap_heal import GapHealScheduler
from ..services.ws.supervisor import WebSocketSupervisor
from ..services.ws.client_server import ClientServer
from ..services.indicator.online import IndicatorOnlineService
from ..services.ws.event_bus import EventBus

async def run_daemon(cfg_path: Path | str = None):
    # 1) 配置与DB
    cfg_path = cfg_path or (Path(__file__).parents[2] / "config" / "app.yaml")
    cfg = load_config(cfg_path)
    client = get_client(cfg.clickhouse)
    ensure_base_tables(client)
    # 集中初始化：默认交易对与K线表
    ensure_bootstrap_defaults(client, cfg.binance.assets)
    # 新增：指标与回测表的集中初始化
    ensure_indicator_tables(client)
    ensure_backtest_tables(client)
    # 初始化指标视图（根据启用的交易对与周期创建）
    pairs = get_enabled_pairs(client)
    ensure_indicator_views_for_pairs(client, pairs, ["1m", "1h"])

    # 4) 组装各服务
    event_bus = EventBus()
    backfill = BackfillManager(cfg, client, event_bus=event_bus)
    scheduler = GapHealScheduler(cfg, client, backfill)
    ws_sup = WebSocketSupervisor(cfg, client, event_bus=event_bus)
    ind_on = IndicatorOnlineService(client)
    client_srv = ClientServer(client, event_bus=event_bus)

    # 5) 启动顺序（所有 DB 初始化后再启动服务）
    await backfill.start()
    await scheduler.start()
    await ws_sup.start_enabled_streams()
    await ind_on.start(pairs, ["1m", "1h"])
    await client_srv.start()
    logger.info("守护已启动：回填/缺口调度/WS/指标在线/ClientServer")

    # 6) 指标日志
    async def _metrics_loop():
        while True:
            snap = backfill.metrics.snapshot()
            logger.info("Metrics: req={} ok={} fail={} rows={}",
                        snap.requests, snap.successes, snap.failures, snap.inserted_rows)
            await asyncio.sleep(60)
    asyncio.create_task(_metrics_loop())

    # 7) 常驻
    try:
        while True:
            await asyncio.sleep(5)
    finally:
        await client_srv.stop()
        await ind_on.stop()
        await scheduler.stop()
        await backfill.stop()