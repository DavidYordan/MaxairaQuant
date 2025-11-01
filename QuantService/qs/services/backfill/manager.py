from __future__ import annotations
import asyncio
from typing import List, Optional, Tuple, Dict
from loguru import logger
import time
from ...buffer.buffer import DataBuffer
from ...common.types import MarketType
from ...config.loader import get_config
from ...gateways.binance_rest import step_ms
from ...services.backfill.fetch import aclose_all_clients
from ...services.backfill.rate_limiter import ApiRateLimiter
from ...services.backfill.fetch import fetch_klines
from ...services.proxy.registry import get_enabled_proxy_url
from ...db.schema import kline_table_name
from ...monitoring.metrics import Metrics
from dataclasses import dataclass

class BackfillManager:
    def __init__(self):
        cfg = get_config()
        self.api_max_limit = cfg.binance.api_max_limit
        self.buffer_batch_size = cfg.buffer.batch_size
        self.buffer_flush_interval_ms = cfg.buffer.flush_interval_ms
        self.limiter = ApiRateLimiter()
        self._markets_using_proxy = {MarketType.spot: False, MarketType.um: False, MarketType.cm: False}
        self._markets_tested = {MarketType.spot: False, MarketType.um: False, MarketType.cm: False}
        self._window_concurrency = cfg.backfill.concurrency_windows
        self.metrics = Metrics()
        self._buffers = {}
        self._market_test_locks = {
            MarketType.spot: asyncio.Lock(),
            MarketType.um: asyncio.Lock(),
            MarketType.cm: asyncio.Lock(),
        }

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

        # 回收 HTTP 客户端池
        await aclose_all_clients()

        logger.info("BackfillManager 已停止")

    async def backfill_gap(self, market: MarketType, symbol: str, period: str, gap_start_ms: int, gap_end_ms: int) -> BackfillSummary:
        """回填数据缺口 - 返回窗口处理摘要"""
        logger.info(f"开始回填缺口：{market} {symbol} {period} 范围={gap_start_ms}~{gap_end_ms}")
        await self._test_market_connection(market, symbol, period)
        proxy_url = await self._select_proxy_url(market)
        table = kline_table_name(symbol, market.value, period)
        windows = self._split_gap(gap_start_ms, gap_end_ms, period)
        if not windows:
            logger.info("没有需要回填的窗口")
            return BackfillSummary(total_rows=0, windows=0, all_ok=True, empty_all=True)

        sem = asyncio.Semaphore(self._adaptive_window_concurrency(table, market))

        results_tasks: List[asyncio.Task[WindowResult]] = []
        async with asyncio.TaskGroup() as tg:
            for i, (ws, we) in enumerate(windows):
                t = tg.create_task(
                    self._do_window_safe(sem, market, symbol, period, table, ws, we, proxy_url),
                    name=f"window_{market}_{symbol}_{period}_{i}"
                )
                results_tasks.append(t)

        # 汇总结果
        results: List[WindowResult] = [t.result() for t in results_tasks]
        total_rows = sum(r.rows_written for r in results)
        all_ok = all(r.ok for r in results)
        empty_all = all(r.rows_written == 0 for r in results)
        logger.info(f"回填完成：{market} {symbol} {period} 处理了 {len(windows)} 个窗口，写入={total_rows} 行")
        return BackfillSummary(total_rows=total_rows, windows=len(windows), all_ok=all_ok, empty_all=empty_all)
    async def _do_window_safe(self, sem: asyncio.Semaphore, market: MarketType, 
                             symbol: str, period: str, table: str, ws: int, we: int, proxy_url: Optional[str]) -> WindowResult:
        """安全的窗口处理包装器，返回窗口结果"""
        try:
            return await self._do_window(sem, market, symbol, period, table, ws, we, proxy_url)
        except Exception as e:
            # 理论上 _do_window 已处理异常，这里兜底
            logger.error(f"窗口处理失败（未捕获异常）：{market} {symbol} {period} [{ws},{we}] 错误={e}")
            return WindowResult(ok=False, rows_written=0, start_ms=ws, end_ms=we)
    async def _do_window(self, sem: asyncio.Semaphore, market: MarketType, 
                     symbol: str, period: str, table: str, ws: int, we: int, proxy_url: Optional[str]) -> WindowResult:
        """处理单个时间窗口的数据回填，返回窗口结果"""
        async with sem:
            await self.limiter.acquire()
            try:
                self.metrics.inc_requests()
                klines, headers = await fetch_klines(
                    market, symbol, period, ws, we, self.api_max_limit, proxy_url
                )
                self.limiter.update_from_headers(headers)
                await self.limiter.maybe_block_by_minute_weight()

                rows = len(klines) if klines else 0
                if rows > 0:
                    buf = await self._ensure_buffer(table)
                    await buf.add_many(klines)
                    self.metrics.inc_successes()
                    self.metrics.add_inserted_rows(rows)
                    logger.debug(
                        f"窗口成功：{market} {symbol} {period} "
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ws/1000))},"
                        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(we/1000))}] 写入 {rows} 行"
                    )
                else:
                    logger.warning(
                        f"窗口无数据：{market} {symbol} {period} "
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ws/1000))},"
                        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(we/1000))}]"
                    )
                return WindowResult(ok=True, rows_written=rows, start_ms=ws, end_ms=we)
            except Exception as e:
                res = await self._handle_window_error(e, market, symbol, period, table, ws, we)
                return res if res is not None else WindowResult(ok=False, rows_written=0, start_ms=ws, end_ms=we)
            finally:
                self.limiter.release()

    async def _handle_window_error(self, error: Exception, market: MarketType, 
                                  symbol: str, period: str, table: str, ws: int, we: int) -> Optional[WindowResult]:
        """处理窗口错误的统一逻辑，返回可选窗口结果（重试成功则返回）"""
        response = getattr(error, "response", None)
        status = response.status_code if response is not None else None

        if status in (403, 451):
            self.enable_proxy_for_market(market)
            await asyncio.sleep(0.5)
            return await self._retry_with_proxy(market, symbol, period, table, ws, we)

        elif status == 429:
            retry_after = self._extract_retry_after(error)
            logger.warning(f"遇到速率限制，等待 {retry_after}s: {market} {symbol} {period} [{ws},{we}]")
            await asyncio.sleep(retry_after)
            return await self._retry_with_proxy(market, symbol, period, table, ws, we)

        elif status in (500, 502, 503, 504):
            await asyncio.sleep(1.0)
            return await self._retry_with_proxy(market, symbol, period, table, ws, we)

        else:
            self.metrics.inc_failures()
            logger.error(
                f"窗口失败：{market} {symbol} {period} [{ws},{we}] 状态={status} 错误={error}"
            )
            return None
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
        # 每次请求最大根数由配置决定
        return step_ms(period) * self.api_max_limit

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
            return await get_enabled_proxy_url()
        return None

    def enable_proxy_for_market(self, market: MarketType):
        """为指定市场启用代理"""
        self._markets_using_proxy[market] = True
        logger.warning(f"启用代理用于市场：{market}")

    async def _test_market_connection(self, market: MarketType, symbol: str, period: str):
        """测试市场连接状态，决定是否需要使用代理"""
        # 并发防重：同一市场的连接测试只允许一次进入
        async with self._market_test_locks[market]:
            if self._markets_tested.get(market, False):
                return

            logger.info(f"正在测试市场连接：{market}")
            end_ms = int(time.time() * 1000)
            start_ms = end_ms - step_ms(period) * 5  # 测试5个周期的数据

            try:
                # 先尝试直连
                await self.limiter.acquire()
                try:
                    klines, headers = await fetch_klines(
                        market, symbol, period, start_ms, end_ms, 5, None
                    )
                    self.limiter.update_from_headers(headers)
                    logger.info(f"市场 {market} 连接正常，无需代理")
                    self._markets_tested[market] = True
                    return
                except Exception as e:
                    response = getattr(e, "response", None)
                    status = response.status_code if response is not None else None

                    if status in (403, 451):  # 地理限制，尝试代理
                        logger.warning(f"市场 {market} 遇到地理限制 (状态码: {status})，启用代理模式")
                        self.enable_proxy_for_market(market)

                        proxy_url = await self._select_proxy_url(market)
                        if proxy_url:
                            klines, headers = await fetch_klines(
                                market, symbol, period, start_ms, end_ms, 5, proxy_url
                            )
                            self.limiter.update_from_headers(headers)
                            logger.info(f"市场 {market} 代理连接测试成功")
                            self._markets_tested[market] = True
                        else:
                            logger.error(f"市场 {market} 无可用代理")
                            raise Exception(f"市场 {market} 无法连接且无可用代理")
                    else:
                        logger.error(f"市场 {market} 连接测试失败，状态码: {status}")
            finally:
                pass
            self._markets_tested[market] = True

    async def _ensure_buffer(self, table: str):
        """确保数据缓冲区存在并已启动"""
        buf = self._buffers.get(table)
        if not buf:
            buf = DataBuffer(
                table_name=table,
                batch_size=self.buffer_batch_size,
                flush_interval_ms=self.buffer_flush_interval_ms
            )
            self._buffers[table] = buf
            await buf.start()
            logger.info(f"创建数据缓冲区: {table}")
        return buf

    async def _retry_with_proxy(self, market: MarketType, symbol: str, period: str, table: str, ws: int, we: int) -> WindowResult:
        await asyncio.sleep(0.5)
        proxy_url = await self._select_proxy_url(market)
        try:
            klines, headers = await fetch_klines(
                market, symbol, period, ws, we, self.api_max_limit, proxy_url
            )
            self.limiter.update_from_headers(headers)
            await self.limiter.maybe_block_by_minute_weight()
            rows = len(klines) if klines else 0
            if rows > 0:
                buf = await self._ensure_buffer(table)
                await buf.add_many(klines)
                self.metrics.inc_successes()
                self.metrics.add_inserted_rows(rows)
            logger.info("代理重试结果：{} {} {} [{},{}] 写入 {} 行", market, symbol, period, ws, we, rows)
            return WindowResult(ok=True, rows_written=rows, start_ms=ws, end_ms=we)
        except Exception as e:
            code = getattr(e, "response", None)
            status = code.status_code if code is not None else None
            self.metrics.inc_failures()
            logger.error("代理重试失败：{} {} {} [{},{}] 状态={} 错误={}", market, symbol, period, ws, we, status, str(e))
            return WindowResult(ok=False, rows_written=0, start_ms=ws, end_ms=we)

    def get_buffer_statuses(self) -> Dict[str, dict]:
        """用于指标采集的缓冲区状态快照"""
        return {table: buf.status() for table, buf in self._buffers.items()}

    def _adaptive_window_concurrency(self, table: str, market: MarketType) -> int:
        """根据系统压力自适应窗口并发数"""
        base = max(1, int(self._window_concurrency))
        concurrency = base

        # 1) 分钟权重压力
        try:
            used = self.limiter.get_used_weight_minute()
            thr = max(1, self.limiter.get_minute_block_threshold())
            ratio = used / thr
            if ratio >= 0.95:
                concurrency = max(1, base // 4)  # 极限降级
            elif ratio >= 0.80:
                concurrency = max(1, base // 2)  # 温和降级
        except Exception:
            pass

        # 2) 缓冲队列压力（针对目标表）
        try:
            buf = self._buffers.get(table)
            if buf:
                st = buf.status()
                qsize = int(st.get("writer_queue_size", 0))
                qcap = int(st.get("max_queue_size", 1))
                pressure = qsize / qcap if qcap > 0 else 0.0
                if pressure >= 0.80:
                    concurrency = 1
                elif pressure >= 0.60:
                    concurrency = max(1, concurrency // 2)
        except Exception:
            pass

        # 3) 故障率（全局粗粒度）
        try:
            req = max(1, int(self.metrics.requests))
            fail = int(self.metrics.failures)
            if req >= 10 and (fail / req) >= 0.15:
                concurrency = max(1, concurrency // 2)
        except Exception:
            pass

        # 4) 代理路径：保守降级
        if self._markets_using_proxy.get(market, False):
            concurrency = max(1, concurrency // 2)

        # 归一化
        concurrency = max(1, min(concurrency, base))
        if concurrency != base:
            logger.info(f"自适应并发：{table} base={base} -> concurrency={concurrency}")
        return concurrency


@dataclass
class WindowResult:
    ok: bool
    rows_written: int
    start_ms: int
    end_ms: int

@dataclass
class BackfillSummary:
    total_rows: int
    windows: int
    all_ok: bool
    empty_all: bool