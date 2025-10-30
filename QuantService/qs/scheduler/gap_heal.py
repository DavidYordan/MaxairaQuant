from __future__ import annotations
import asyncio
import time
from typing import Dict, List
from loguru import logger
from ..config.schema import AppConfig
from ..db.schema import kline_table_name
from ..db.queries import get_enabled_pairs, find_gaps_windowed_sql, get_max_open_time
from ..services.backfill.manager import BackfillManager
from ..gateways.binance_rest import step_ms
from ..common.types import MarketType
from ..db.client import AsyncClickHouseClient


class GapHealScheduler:
    """
    水位驱动的缺口修复调度器

    - 不做历史全量初扫，不维护初扫状态
    - 每轮按周期对每个交易对计算水位（max(open_time)），在水位附近做小范围回看（lookback）
      并扫描到最新闭合K线
    - 仅对真实缺口派发回填任务，避免重复写入
    """

    def __init__(self, cfg: AppConfig, ch_client: AsyncClickHouseClient, backfill: BackfillManager):
        self.cfg = cfg
        self.client = ch_client
        self.backfill = backfill

        self._tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

        # 并发控制
        self._scan_pair_concurrency = 4                 # 并发扫描交易对数量
        self._task_semaphore = asyncio.Semaphore(8)     # 回填任务最大并发数

        # 去重控制：以“缺口范围”为维度标记
        self._running_keys: Dict[str, bool] = {}

        # 回看步数：容忍迟到写入与小范围波动（可按需调整）
        self._lookback_steps = 5

    async def start(self):
        if self._tasks:
            logger.warning("GapHealScheduler 已在运行")
            return

        self._shutdown_event.clear()
        tasks_config = [
            ("1m", 30),   # 每30秒扫一次1m
            ("1h", 300),  # 每5分钟扫一次1h
        ]
        for period, interval_s in tasks_config:
            task = asyncio.create_task(self._loop(period, interval_s), name=f"gap_heal_{period}")
            self._tasks.append(task)

        logger.info("GapHealScheduler 启动完成（1m/30s，1h/300s）")

    async def stop(self):
        if not self._tasks:
            return

        logger.info("正在停止 GapHealScheduler...")
        self._shutdown_event.set()

        for task in self._tasks:
            if not task.done():
                task.cancel()

        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning("部分任务未能在超时时间内完成")

        self._tasks.clear()
        self._running_keys.clear()
        logger.info("GapHealScheduler 已停止")

    async def _loop(self, period: str, interval_s: int):
        consecutive_errors = 0
        max_consecutive_errors = 5

        while not self._shutdown_event.is_set():
            try:
                await self._scan_period(period)
                consecutive_errors = 0
            except asyncio.CancelledError:
                logger.info(f"Gap heal loop {period} 被取消")
                break
            except Exception as e:
                consecutive_errors += 1
                logger.exception(f"GapHealScheduler[{period}] 异常(第{consecutive_errors}次): {e}")
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"GapHealScheduler[{period}] 连续错误过多，停止该周期")
                    break
                await asyncio.sleep(min(interval_s * (2 ** (consecutive_errors - 1)), 300))

            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=interval_s)
                break
            except asyncio.TimeoutError:
                continue

    async def _scan_period(self, period: str):
        s_ms = step_ms(period)

        # 将结束时间对齐到“最后一根已闭合K线”的开盘时间，避免尾部伪缺口
        now_ms = int(time.time() * 1000)
        end_ms = (now_ms // s_ms) * s_ms - s_ms
        if end_ms <= 0:
            return

        # 获取启用交易对（带重试）
        pairs = await self._get_enabled_pairs_with_retry()

        # 限制并发扫描，避免对 CH 产生过多并发查询
        sem = asyncio.Semaphore(min(self._scan_pair_concurrency, max(1, len(pairs))))
        tasks: List[asyncio.Task] = []
        for symbol, market in pairs:
            task = asyncio.create_task(
                self._scan_pair(sem, symbol, market, period, s_ms, end_ms),
                name=f"scan_{market}_{symbol}_{period}"
            )
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    symbol, market = pairs[i]
                    logger.error(f"扫描交易对失败 {market} {symbol} {period}: {result}")

    async def _get_enabled_pairs_with_retry(self, max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                return await get_enabled_pairs(self.client)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"获取交易对失败，已重试{max_retries}次: {e}")
                    raise
                logger.warning(f"获取交易对失败，第{attempt + 1}次重试: {e}")
                await asyncio.sleep(1.0 * (attempt + 1))

    async def _scan_pair(self, sem: asyncio.Semaphore, symbol: str, market: str, period: str, s_ms: int, end_ms: int):
        async with sem:
            table = kline_table_name(symbol, market, period)

            # 读取水位：当前表内最大 open_time
            try:
                last_ot = await get_max_open_time(self.client, table)
            except Exception as e:
                logger.error(f"读取水位失败 {market} {symbol} {period}: {e}")
                return

            # 计算扫描起点：从水位回看少量步数，容忍迟到写入
            if last_ot and last_ot > 0:
                start_ms = max(last_ot - self._lookback_steps * s_ms, 0)
            else:
                # 表为空：从配置中的历史起点开始
                try:
                    start_ms = int(time.mktime(time.strptime(self.cfg.binance.historical_start_dates, "%Y-%m-%d")) * 1000)
                except Exception:
                    # 配置异常时，退化为回看 7 天
                    start_ms = end_ms - 7 * 24 * 60 * 60 * 1000

            start_ms = min(start_ms, end_ms)
            if start_ms >= end_ms:
                return

            # 查找缺口
            try:
                gaps = await find_gaps_windowed_sql(self.client, table, start_ms, end_ms, s_ms)
            except Exception as e:
                logger.error(f"缺口查询失败 {market} {symbol} {period}: {e}")
                return

            if not gaps:
                logger.debug(f"无缺口：{market} {symbol} {period} 范围={start_ms}~{end_ms}")
                return

            # 按缺口维度派发回填任务（包含范围去重）
            for gs, ge in gaps:
                key = f"{market}|{symbol}|{period}|{gs}|{ge}"
                if self._running_keys.get(key, False):
                    continue

                self._running_keys[key] = True
                asyncio.create_task(
                    self._dispatch_and_release(key, market, symbol, period, gs, ge),
                    name=f"heal_{market}_{symbol}_{period}_{gs}_{ge}"
                )

    async def _dispatch_and_release(self, key: str, market: str, symbol: str, period: str, gs: int, ge: int):
        try:
            # 将并发限制用于实际执行阶段，避免任务泛滥
            async with self._task_semaphore:
                logger.info(f"开始回填缺口：{market} {symbol} {period} [{gs},{ge}]")
                await self.backfill.backfill_gap(
                    MarketType(market), symbol, period, gs, ge
                )
                logger.info(f"回填完成：{market} {symbol} {period} [{gs},{ge}]")
        except Exception as e:
            logger.error(f"回填失败：{market} {symbol} {period} [{gs},{ge}] 错误={e}")
        finally:
            self._running_keys.pop(key, None)