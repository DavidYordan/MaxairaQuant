import asyncio
from pathlib import Path
from loguru import logger
from ..config.loader import load_config
from ..db.client import ClickHouseClientManager
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
    clients = ClickHouseClientManager(cfg.clickhouse)

    read_cli = await clients.get_read()
    write_cli = await clients.get_write()

    await ensure_base_tables(write_cli)
    await ensure_bootstrap_defaults(write_cli, cfg.binance.assets)
    await ensure_indicator_tables(write_cli)
    await ensure_backtest_tables(write_cli)

    pairs = await get_enabled_pairs(read_cli)
    await ensure_indicator_views_for_pairs(write_cli, pairs, ["1m", "1h"])

    # 4) 组装各服务
    event_bus = EventBus()
    backfill = BackfillManager(cfg, clients, read_cli)
    scheduler = GapHealScheduler(cfg, read_cli, backfill)
    ws_sup = WebSocketSupervisor(cfg, read_cli, write_cli, event_bus=event_bus)
    ind_on = IndicatorOnlineService(read_cli, write_cli)
    client_srv = ClientServer(read_cli, event_bus=event_bus)

    # 5) 启动顺序（所有 DB 初始化后再启动服务）
    await backfill.start()
    await scheduler.start()
    await ws_sup.start_enabled_streams()
    await ind_on.start(pairs, ["1m", "1h"])
    await client_srv.start()
    logger.info("守护已启动：回填/缺口调度/WS/指标在线/ClientServer")

    # 6) 指标日志
    # 事件循环延迟监测：每秒采样一次，metrics loop 每60秒汇总
    loop_delay_stats = {"sum": 0.0, "count": 0, "max": 0.0}
    async def _loop_delay_monitor():
        loop = asyncio.get_running_loop()
        interval = 1.0
        next_t = loop.time() + interval
        while True:
            await asyncio.sleep(interval)
            now = loop.time()
            delay = max(0.0, now - next_t)  # 秒
            loop_delay_stats["sum"] += delay
            loop_delay_stats["count"] += 1
            if delay > loop_delay_stats["max"]:
                loop_delay_stats["max"] = delay
            next_t = now + interval

    async def _metrics_loop():
        while True:
            snap = backfill.metrics.snapshot()
            # DataBuffer 堆积（Backfill + WS）
            bf_buf_stats = backfill.get_buffer_statuses()

            def agg(buf_stats: dict):
                total_q = sum(s.get("writer_queue_size", 0) for s in buf_stats.values())
                max_q = max([s.get("writer_queue_size", 0) for s in buf_stats.values()] or [0])
                workers = sum(s.get("writer_active_workers", 0) for s in buf_stats.values())
                return total_q, max_q, workers

            bf_total_q, bf_max_q, bf_workers = agg(bf_buf_stats)

            # EventBus 统计
            eb_stats = event_bus.get_stats()
            eb_topics = eb_stats.get("topics", 0)
            eb_subs = eb_stats.get("subscribers", 0)
            eb_queues = eb_stats.get("queues", 0)
            eb_dropped = eb_stats.get("dropped", 0)
            eb_delivered = eb_stats.get("delivered", 0)

            # 事件循环延迟（ms）
            cnt = max(1, loop_delay_stats["count"])
            avg_ms = (loop_delay_stats["sum"] / cnt) * 1000.0
            max_ms = loop_delay_stats["max"] * 1000.0
            # 重置采样
            loop_delay_stats["sum"] = 0.0
            loop_delay_stats["count"] = 0
            loop_delay_stats["max"] = 0.0

            logger.info(
                "Metrics: req={} ok={} fail={} rows={} | loop_delay(avg_ms={:.1f}, max_ms={:.1f}) | "
                "BackfillQ(total={}, max={}, workers={}) | EventBus(topics={}, subs={}, queues={}, delivered={}, dropped={})",
                snap.requests, snap.successes, snap.failures, snap.inserted_rows,
                avg_ms, max_ms,
                bf_total_q, bf_max_q, bf_workers,
                eb_topics, eb_subs, eb_queues, eb_delivered, eb_dropped
            )
            await asyncio.sleep(60)

    delay_task = asyncio.create_task(_loop_delay_monitor())
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
        # 关闭监控任务与数据库连接
        delay_task.cancel()
        try:
            await clients.read.close()
            await clients.write.close()
            await clients.backfill.close()
        except Exception as e:
            logger.warning("关闭 ClickHouse 客户端失败: {}", e)