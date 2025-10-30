from __future__ import annotations
import asyncio
import time
from typing import Dict, List
from loguru import logger
from ..config.schema import AppConfig
from ..db.schema import kline_table_name
from ..db.queries import get_enabled_pairs, find_gaps_windowed_sql
from ..services.backfill.manager import BackfillManager
from ..gateways.binance_rest import step_ms
from ..common.types import MarketType
from ..db.client import AsyncClickHouseClient

class GapHealScheduler:
    """数据缺口修复调度器 - 改进的异步任务管理"""
    
    def __init__(self, cfg: AppConfig, ch_client: AsyncClickHouseClient, backfill: BackfillManager):
        self.cfg = cfg
        self.client = ch_client
        self.backfill = backfill
        self._tasks: List[asyncio.Task] = []
        self._running_keys: Dict[str, bool] = {}
        self._shutdown_event = asyncio.Event()
        self._task_semaphore = asyncio.Semaphore(10)  # 限制并发任务数

    async def start(self):
        """启动调度器"""
        if self._tasks:
            logger.warning("GapHealScheduler 已经在运行")
            return
            
        self._shutdown_event.clear()
        
        # 创建调度任务
        tasks_config = [
            ("1m", 30),   # 1分钟周期，每30秒扫描
            ("1h", 300),  # 1小时周期，每5分钟扫描
        ]
        
        for period, interval_s in tasks_config:
            task = asyncio.create_task(
                self._loop_with_error_handling(period, interval_s),
                name=f"gap_heal_{period}"
            )
            self._tasks.append(task)
            
        logger.info("GapHealScheduler 已启动（1m/30s，1h/300s）")

    async def stop(self):
        """优雅停止调度器"""
        if not self._tasks:
            return
            
        logger.info("正在停止 GapHealScheduler...")
        self._shutdown_event.set()
        
        # 取消所有任务
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # 等待任务完成或超时
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

    async def _loop_with_error_handling(self, period: str, interval_s: int):
        """带错误处理的循环任务"""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while not self._shutdown_event.is_set():
            try:
                await self._scan_and_heal(period, interval_s)
                consecutive_errors = 0  # 重置错误计数
                
            except asyncio.CancelledError:
                logger.info(f"Gap heal loop {period} 被取消")
                break
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"GapHealScheduler {period} 异常 (连续第{consecutive_errors}次): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"GapHealScheduler {period} 连续错误过多，停止运行")
                    break
                    
                # 指数退避
                error_sleep = min(interval_s * (2 ** (consecutive_errors - 1)), 300)
                await asyncio.sleep(error_sleep)
                continue
            
            # 正常间隔等待
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), 
                    timeout=interval_s
                )
                break  # 收到停止信号
            except asyncio.TimeoutError:
                continue  # 超时继续下一轮

    async def _scan_and_heal(self, period: str, interval_s: int):
        """扫描并修复缺口"""
        s_ms = step_ms(period)
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - 60 * s_ms  # 60分钟窗口
        
        # 获取启用的交易对
        pairs = await get_enabled_pairs(self.client)
        
        # 并发处理所有交易对
        scan_tasks = []
        for symbol, market in pairs:
            task = asyncio.create_task(
                self._scan_pair(symbol, market, period, start_ms, end_ms, s_ms),
                name=f"scan_{market}_{symbol}_{period}"
            )
            scan_tasks.append(task)
        
        if scan_tasks:
            await asyncio.gather(*scan_tasks, return_exceptions=True)

    async def _scan_pair(self, symbol: str, market: str, period: str, 
                        start_ms: int, end_ms: int, s_ms: int):
        """扫描单个交易对的缺口"""
        try:
            table = kline_table_name(symbol, market, period)
            gaps = await find_gaps_windowed_sql(self.client, table, start_ms, end_ms, s_ms)
            
            if gaps:
                logger.info(f"发现缺口：{market} {symbol} {period} 数量={len(gaps)}")
                
            for (gs, ge) in gaps:
                key = f"{market}|{symbol}|{period}"
                
                # 检查是否已在运行
                if self._running_keys.get(key, False):
                    continue
                    
                # 使用信号量限制并发
                async with self._task_semaphore:
                    self._running_keys[key] = True
                    logger.info(f"开始填充缺口：{market} {symbol} {period} 范围={gs}~{ge}")
                    
                    # 创建独立的修复任务
                    asyncio.create_task(
                        self._dispatch_and_release(key, market, symbol, period, gs, ge),
                        name=f"heal_{key}_{gs}_{ge}"
                    )
                    
        except Exception as e:
            logger.error(f"扫描交易对失败 {market} {symbol} {period}: {e}")

    async def _dispatch_and_release(self, key: str, market: str, symbol: str, 
                                  period: str, gs: int, ge: int):
        """分发修复任务并释放锁"""
        try:
            await self.backfill.backfill_gap(MarketType(market), symbol, period, gs, ge)
            logger.info(f"缺口修复完成：{key} 范围={gs}~{ge}")
            
        except Exception as e:
            logger.error(f"缺口修复失败：{key} 范围={gs}~{ge} 错误={e}")
            
        finally:
            # 确保释放锁
            self._running_keys[key] = False