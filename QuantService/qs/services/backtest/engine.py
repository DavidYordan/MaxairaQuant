from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Dict, Any, List
from clickhouse_connect.driver.client import Client
from db.schema import kline_table_name

@dataclass
class Bar:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float

class Strategy:
    def __init__(self, params: Dict[str, Any]):
        self.params = params

    def on_bars(self, bars: List[Bar]) -> List[Dict[str, Any]]:
        # 返回订单/信号列表；示例：占位
        return []

    def finalize(self) -> Dict[str, Any]:
        return {"pnl": 0.0, "sharpe": 0.0, "trades": []}

class WindowReader:
    def __init__(self, client: Client):
        self.client = client

    def read(self, symbol: str, market: str, period: str, start_ms: int, end_ms: int) -> List[Bar]:
        table = kline_table_name(symbol, market, period)
        rs = self.client.query(
            f"""
            SELECT open_time, toFloat64(open), toFloat64(high), toFloat64(low), toFloat64(close), toFloat64(volume)
            FROM {table}
            WHERE open_time >= %(s)s AND open_time <= %(e)s
            ORDER BY open_time
            """,
            params={"s": start_ms, "e": end_ms},
        )
        return [Bar(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])) for r in rs.result_rows]

class BacktestEngine:
    def __init__(self, client: Client):
        self.client = client
        self._ensure_result_table()

    def _ensure_result_table(self) -> None:
        self.client.command(
            """
            CREATE TABLE IF NOT EXISTS backtest_results
            (
              job_id String,
              symbol String,
              market String,
              period String,
              params_json String,
              metric_pnl Float64,
              metric_sharpe Float64,
              trades_json String,
              started_at DateTime DEFAULT now(),
              finished_at DateTime DEFAULT now()
            ) ENGINE = MergeTree
            ORDER BY (job_id, symbol, market, period)
            """
        )

    async def run(self, job_id: str, strategy: Strategy, symbol: str, market: str, period: str, start_ms: int, end_ms: int) -> None:
        reader = WindowReader(self.client)
        bars = reader.read(symbol, market, period, start_ms, end_ms)
        signals = strategy.on_bars(bars)
        metrics = strategy.finalize()
        await asyncio.to_thread(
            self.client.command,
            """
            INSERT INTO backtest_results (job_id, symbol, market, period, params_json, metric_pnl, metric_sharpe, trades_json)
            VALUES (%(job_id)s, %(symbol)s, %(market)s, %(period)s, %(params)s, %(pnl)s, %(sharpe)s, %(trades)s)
            """,
            params={
                "job_id": job_id,
                "symbol": symbol,
                "market": market,
                "period": period,
                "params": json.dumps(strategy.params, ensure_ascii=False),
                "pnl": float(metrics.get("pnl", 0.0)),
                "sharpe": float(metrics.get("sharpe", 0.0)),
                "trades": json.dumps(metrics.get("trades", []), ensure_ascii=False),
            },
        )