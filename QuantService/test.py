import httpx
import asyncio

PROXY = "socks5://i4z5B2Y9S1I4:s9h3y8c0P5F7@103.129.161.230:7778"

# Binance K线接口
URL = "https://fapi.binance.com/fapi/v1/klines"
PARAMS = {
    "symbol": "ETHUSDT",
    "interval": "1m",
    "startTime": 3840060000,
    "endTime": 3900059999,
    "limit": 1000,
}

async def main():
    async with httpx.AsyncClient(proxies=PROXY, timeout=10) as client:
        try:
            resp = await client.get(URL, params=PARAMS)
            print(f"状态码: {resp.status_code}")
            print("响应内容:")
            print(resp.text[:500])  # 打印前500个字符以防太长
        except Exception as e:
            print("请求出错:", e)


if __name__ == "__main__":
    asyncio.run(main())
