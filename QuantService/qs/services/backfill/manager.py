from __future__ import annotations
import asyncio
from typing import List, Optional, Tuple, Dict
from loguru import logger
from ...buffer.buffer import DataBuffer
from ...common.types import MarketType
from ...config.schema import AppConfig
from ...gateways.binance_rest import step_ms
from ...services.backfill.rate_limiter import ApiRateLimiter
from ...services.backfill.fetch import fetch_klines
from ...services.proxy.registry import get_enabled_proxy_url
from ...db.schema import kline_table_name
from ...monitoring.metrics import Metrics
from ...services.ws.event_bus import EventBus
from ...db.client import AsyncClickHouseClient

class BackfillManager:
    def __init__(self, cfg: AppConfig, ch_client: AsyncClickHouseClient, event_bus: Optional[EventBus] = None):
        self.cfg = cfg
        self.client = ch_client
        self.limiter = ApiRateLimiter(cfg.binance.requests_per_second)
        self._markets_using_proxy: Dict[MarketType, bool] = {MarketType.spot: False, MarketType.um: False, MarketType.cm: False}
        self._markets_tested: Dict[MarketType, bool] = {MarketType.spot: False, MarketType.um: False, MarketType.cm: False}
        self._window_concurrency = cfg.backfill.concurrency_windows
        self.metrics = Metrics()
        self._buffers: Dict[str, DataBuffer] = {}
        self._event_bus = event_bus

    async def start(self):
        """启动回填管理器"""
        await self.limiter.start()
        logger.info("BackfillManager 已启动")
        
    async def stop(self):
        """停止回填管理器并清理资源"""
        logger.info("正在停止 BackfillManager...")
        
        # 停止限流器
        await self.limiter.stop()
        
        # 停止所有缓冲区
        stop_tasks = []
        for table, buffer in self._buffers.items():
            task = asyncio.create_task(
                buffer.stop(),
                name=f"stop_buffer_{table}"
            )
            stop_tasks.append(task)
        
        if stop_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning("部分缓冲区停止超时")
        
        self._buffers.clear()
        logger.info("BackfillManager 已停止")

    async def backfill_gap(self, market: MarketType, symbol: str, period: str, gap_start_ms: int, gap_end_ms: int):
        """回填数据缺口 - 改进的并发控制和错误处理"""
        logger.info(f"开始回填缺口：{market} {symbol} {period} 范围={gap_start_ms}~{gap_end_ms}")
        
        # 首先进行连接探测
        await self._test_market_connection(market, symbol, period)
        
        table = kline_table_name(symbol, market.value, period)
        windows = self._split_gap(gap_start_ms, gap_end_ms, period)
        
        if not windows:
            logger.info("没有需要回填的窗口")
            return
        
        # 使用信号量控制并发
        sem = asyncio.Semaphore(self._window_concurrency)
        
        # 创建任务组
        async with asyncio.TaskGroup() as tg:
            tasks = []
            for i, (ws, we) in enumerate(windows):
                task = tg.create_task(
                    self._do_window_safe(sem, market, symbol, period, table, ws, we),
                    name=f"window_{market}_{symbol}_{period}_{i}"
                )
                tasks.append(task)
        
        logger.info(f"回填完成：{market} {symbol} {period} 处理了 {len(windows)} 个窗口")

    async def _do_window_safe(self, sem: asyncio.Semaphore, market: MarketType, 
                             symbol: str, period: str, table: str, ws: int, we: int):
        """安全的窗口处理包装器"""
        try:
            await self._do_window(sem, market, symbol, period, table, ws, we)
        except Exception as e:
            logger.error(f"窗口处理失败：{market} {symbol} {period} [{ws},{we}] 错误={e}")
            # 不重新抛出异常，让其他窗口继续处理

    async def _do_window(self, sem: asyncio.Semaphore, market: MarketType, 
                        symbol: str, period: str, table: str, ws: int, we: int):
        """处理单个时间窗口的数据回填"""
        async with sem:  # 控制并发数
            # 获取API限流令牌
            await self.limiter.acquire()
            
            try:
                proxy_url = await self._select_proxy_url(market)
                self.metrics.inc_requests()
                
                # 获取数据
                klines, headers = await fetch_klines(
                    self.cfg, market, symbol, period, ws, we, 1000, proxy_url
                )
                
                # 更新限流器状态
                self.limiter.update_from_headers(headers)
                await self.limiter.maybe_block_by_minute_weight()
                
                # 写入数据
                if klines:
                    buf = await self._ensure_buffer(table)
                    await buf.add_many(klines)
                    
                    self.metrics.inc_successes()
                    self.metrics.add_inserted_rows(len(klines))
                    logger.debug(
                        f"窗口成功：{market} {symbol} {period} [{ws},{we}] 写入 {len(klines)} 行"
                    )
                else:
                    logger.warning(f"窗口无数据：{market} {symbol} {period} [{ws},{we}]")
                    
            except Exception as e:
                await self._handle_window_error(e, market, symbol, period, table, ws, we)
            finally:
                self.limiter.release()

    async def _handle_window_error(self, error: Exception, market: MarketType, 
                                  symbol: str, period: str, table: str, ws: int, we: int):
        """处理窗口错误的统一逻辑"""
        response = getattr(error, "response", None)
        status = response.status_code if response is not None else None
        
        # 根据错误类型决定处理策略
        if status in (403, 451):  # 地理限制或禁止访问
            self.enable_proxy_for_market(market)
            await asyncio.sleep(0.5)
            await self._retry_with_proxy(market, symbol, period, table, ws, we)
            
        elif status == 429:  # 速率限制
            retry_after = self._extract_retry_after(error)
            logger.warning(
                f"遇到速率限制，等待 {retry_after}s: {market} {symbol} {period} [{ws},{we}]"
            )
            await asyncio.sleep(retry_after)
            await self._retry_with_proxy(market, symbol, period, table, ws, we)
            
        elif status in (500, 502, 503, 504):  # 服务器错误，可重试
            await asyncio.sleep(1.0)
            await self._retry_with_proxy(market, symbol, period, table, ws, we)
            
        else:  # 其他错误，记录但不重试
            self.metrics.inc_failures()
            logger.error(
                f"窗口失败：{market} {symbol} {period} [{ws},{we}] "
                f"状态={status} 错误={error}"
            )

    def _extract_retry_after(self, error: Exception) -> float:
        """从错误响应中提取重试延迟时间"""
        try:
            if hasattr(error, 'response') and error.response:
                retry_after = error.response.headers.get("Retry-After", "1")
                return float(retry_after)
        except (ValueError, AttributeError):
            pass
        return 1.0  # 默认1秒

    def _window_size_ms(self, period: str) -> int:
        """计算窗口大小（毫秒）"""
        return step_ms(period) * 1000  # 使用1000作为默认API限制

    def _split_gap(self, start_ms: int, end_ms: int, period: str) -> List[Tuple[int, int]]:
        """将缺口分割为多个时间窗口"""
        size = self._window_size_ms(period)
        windows: List[Tuple[int, int]] = []
        cur = start_ms
        while cur <= end_ms:
            nxt = min(cur + size - 1, end_ms)
            windows.append((cur, nxt))
            cur = nxt + 1
        return windows

    async def _select_proxy_url(self, market: MarketType) -> Optional[str]:
        """选择代理URL"""
        if self._markets_using_proxy.get(market, False):
            return await get_enabled_proxy_url(self.client)
        return None

    def enable_proxy_for_market(self, market: MarketType):
        """为指定市场启用代理"""
        self._markets_using_proxy[market] = True
        logger.warning(f"启用代理用于市场：{market}")

    async def _test_market_connection(self, market: MarketType, symbol: str, period: str):
        """测试市场连接状态，决定是否需要使用代理"""
        if hasattr(self, '_markets_tested') and self._markets_tested.get(market, False):
            logger.debug(f"市场 {market} 已测试过连接状态")
            return
            
        if not hasattr(self, '_markets_tested'):
            self._markets_tested = {}
            
        logger.info(f"正在测试市场连接：{market}")
        
        # 使用一个小的时间窗口进行测试
        import time
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - step_ms(period) * 5  # 测试5个周期的数据
        
        try:
            # 先尝试不使用代理
            await self.limiter.acquire()
            try:
                klines, headers = await fetch_klines(
                    self.cfg, market, symbol, period, start_ms, end_ms, 5, None
                )
                self.limiter.update_from_headers(headers)
                logger.info(f"市场 {market} 连接正常，无需代理")
                self._markets_tested[market] = True
                return
                
            except Exception as e:
                response = getattr(e, "response", None)
                status = response.status_code if response is not None else None
                
                if status in (403, 451):  # 地理限制
                    logger.warning(f"市场 {market} 遇到地理限制 (状态码: {status})，启用代理模式")
                    self.enable_proxy_for_market(market)
                    
                    # 测试代理连接
                    proxy_url = await self._select_proxy_url(market)
                    if proxy_url:
                        klines, headers = await fetch_klines(
                            self.cfg, market, symbol, period, start_ms, end_ms, 5, proxy_url
                        )
                        self.limiter.update_from_headers(headers)
                        logger.info(f"市场 {market} 代理连接测试成功")
                    else:
                        logger.error(f"市场 {market} 无可用代理")
                        raise Exception(f"市场 {market} 无法连接且无可用代理")
                else:
                    logger.error(f"市场 {market} 连接测试失败，状态码: {status}")
                    raise
                    
        finally:
            self.limiter.release()
            
        self._markets_tested[market] = True

    async def _ensure_buffer(self, table: str):
        """确保数据缓冲区存在并已启动"""
        buf = self._buffers.get(table)
        if not buf:
            # 创建缓冲区，使用优化的默认参数
            buf = DataBuffer(
                client=self.client, 
                table_name=table, 
                batch_size=3000,           # 回填使用更大批次
                flush_interval_ms=2000,    # 2秒刷新间隔
                event_bus=self._event_bus,
                writer_workers=6,          # 回填使用更多写入器
                max_queue_size=30000       # 更大队列应对批量数据
            )
            self._buffers[table] = buf
            await buf.start()
            logger.info(f"创建数据缓冲区: {table}")
        return buf

    async def _retry_with_proxy(self, market: MarketType, symbol: str, period: str, table: str, ws: int, we: int):
        await asyncio.sleep(0.5)
        proxy_url = await self._select_proxy_url(market)
        try:
            klines, headers = await fetch_klines(self.cfg, market, symbol, period, ws, we, 1000, proxy_url)
            self.limiter.update_from_headers(headers)
            await self.limiter.maybe_block_by_minute_weight()
            buf = await self._ensure_buffer(table)
            await buf.add_many(klines)
            self.metrics.inc_successes()
            self.metrics.add_inserted_rows(len(klines))
            logger.info("代理重试成功：{} {} {} [{},{}] 写入 {} 行", market, symbol, period, ws, we, len(klines))
        except Exception as e:
            code = getattr(e, "response", None)
            status = code.status_code if code is not None else None
            self.metrics.inc_failures()
            logger.error("代理重试失败：{} {} {} [{},{}] 状态={} 错误={}", market, symbol, period, ws, we, status, str(e))