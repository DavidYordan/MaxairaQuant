from pathlib import Path
import requests, zipfile, io, gc
import pandas as pd
from clickhouse_connect import get_client

BASE = "https://data.binance.vision"
SYMBOL = "ETHUSDT"
UMCM = "um"  # USDⓈ-M Futures
DATA_DIR = Path("/www/wwwroot/MaxairaQuant/data/usdt_m/aggTrades") / SYMBOL
DATA_DIR.mkdir(parents=True, exist_ok=True)

client = get_client(host="localhost", port=8123, database="binance")

def download_and_insert(year: int, month: int):
    ym = f"{year}-{month:02d}"
    zip_path = DATA_DIR / f"{SYMBOL}-aggTrades-{ym}.zip"
    url = f"{BASE}/data/futures/{UMCM}/monthly/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{ym}.zip"

    print(f"[INFO] downloading {url}")
    try:
        r = requests.get(url, timeout=120)
    except Exception as e:
        print(f"[ERROR] download failed: {e}")
        return

    if r.status_code != 200:
        print(f"[WARN] skip {ym}, not found or error {r.status_code}")
        return

    zip_path.write_bytes(r.content)

    total_rows = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            print(f"[INFO] extracting {name}")
            with zf.open(name) as f:
                # 分块读取，每块 100 万行
                for chunk in pd.read_csv(f, header=None, dtype=str, chunksize=1_000_000, low_memory=False):
                    # 识别列结构
                    if chunk.shape[1] == 8:
                        chunk.columns = ["agg_id","price","qty","firstId","lastId","ts","is_buyer_maker","isBestMatch"]
                    elif chunk.shape[1] == 7:
                        chunk.columns = ["agg_id","price","qty","firstId","lastId","ts","is_buyer_maker"]
                    else:
                        print(f"[WARN] unexpected column count {chunk.shape[1]} in {name}")
                        continue

                    # 只保留必要字段
                    df = chunk[["ts","price","qty","agg_id","is_buyer_maker"]].copy()

                    # 转换类型（固定毫秒时间戳）
                    df["ts"] = pd.to_datetime(pd.to_numeric(df["ts"], errors="coerce"), unit="ms", utc=True, errors="coerce")
                    df["price"] = pd.to_numeric(df["price"], errors="coerce")
                    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
                    df["agg_id"] = pd.to_numeric(df["agg_id"], errors="coerce", downcast="integer")
                    df["is_buyer_maker"] = df["is_buyer_maker"].map({"true": 1, "false": 0}).astype("uint8")

                    df.dropna(subset=["ts", "price", "qty", "agg_id"], inplace=True)

                    if not df.empty:
                        client.insert_df("ethusdt", df[["ts", "price", "qty", "agg_id", "is_buyer_maker"]])
                        total_rows += len(df)
                        print(f"[INFO] inserted chunk ({len(df)} rows)")

                    # 释放内存
                    del df, chunk
                    gc.collect()

    print(f"[OK] {ym} total_rows={total_rows} inserted")

if __name__ == "__main__":
    for y in range(2024, 2026):
        for m in range(1, 10 if y == 2025 else 13):
            download_and_insert(y, m)