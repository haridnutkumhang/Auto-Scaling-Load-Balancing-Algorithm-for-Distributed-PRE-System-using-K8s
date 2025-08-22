import argparse, asyncio, aiohttp, time, statistics
import os, io

MASTER_URL = None
FILE_BYTES = None

async def send_file(session, sem, latencies, errors):
    async with sem:
        start = time.perf_counter()
        data = aiohttp.FormData()
        data.add_field(
            name="file",
            value=io.BytesIO(FILE_BYTES),
            filename="request.py",
            content_type="application/octet-stream",
        )
        try:
            async with session.post(MASTER_URL, data=data) as resp:
                await resp.text()
        except Exception as e:
            errors.append(str(e))
            return
        end = time.perf_counter()
        latencies.append((end - start) * 1000)

async def run(total_requests: int, concurrency: int):
    sem = asyncio.Semaphore(concurrency)
    latencies = []
    errors = []
    connector = aiohttp.TCPConnector(limit=concurrency)
    start = time.perf_counter()
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(send_file(session, sem, latencies, errors))
            for _ in range(total_requests)
        ]
        await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start

    success = len(latencies)
    throughput = success / elapsed if elapsed > 0 else 0
    latencies.sort()
    def pct(p): return latencies[int(len(latencies)*p) - 1] if latencies else 0
    p50, p95, p99 = pct(0.50), pct(0.95), pct(0.99)

    print("\n=== Load Test Results ===")
    print(f"Total Requests    : {total_requests}")
    print(f"Successful        : {success}")
    print(f"Failed            : {len(errors)}")
    print(f"Total Time        : {elapsed:.2f} s")
    print(f"Throughput        : {throughput:.1f} req/s")
    print(f"Latency p50       : {p50:.1f} ms")
    print(f"Latency p95       : {p95:.1f} ms")
    print(f"Latency p99       : {p99:.1f} ms")

def main():
    global MASTER_URL, FILE_BYTES
    p = argparse.ArgumentParser()
    p.add_argument("-u","--url",      required=True)
    p.add_argument("-f","--file",     required=True)
    p.add_argument("-n","--total",    type=int, default=10000)
    p.add_argument("-c","--concurrent", type=int, default=200)
    args = p.parse_args()

    MASTER_URL = args.url
    # อ่านไฟล์เป็น bytes แค่ครั้งเดียว
    with open(args.file, "rb") as f:
        FILE_BYTES = f.read()

    asyncio.run(run(args.total, args.concurrent))

if __name__ == "__main__":
    main()
