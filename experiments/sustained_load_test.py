"""Run paced HTTP requests independently of response completion.

Usage: python3 experiments/sustained_load_test.py
Uses only the Python standard library.
"""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import math
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, ProxyHandler


def percentile(values, fraction):
    """Return a nearest-rank percentile in milliseconds, or None for no samples."""
    if not values:
        return None
    return round(sorted(values)[math.ceil(len(values) * fraction) - 1], 2)


def run_stage(url, rate, duration, concurrency):
    """Pace arrivals at a fixed rate and report successes, rejections, and missed send slots."""
    slots = threading.BoundedSemaphore(concurrency)
    results = []
    lock = threading.Lock()
    local = threading.local()
    started = 0
    skipped = 0

    def send():
        """Send one request without retries and record its status and elapsed time."""
        start = time.perf_counter()
        status = "client_error"
        try:
            if not hasattr(local, "opener"):
                local.opener = build_opener(ProxyHandler({}))
            request = Request(url, data=b'{"x": 3}',
                              headers={"Content-Type": "application/json"})
            try:
                with local.opener.open(request, timeout=35) as response:
                    response.read()
                    status = str(response.status)
            except HTTPError as error:
                status = str(error.code)
                error.close()
        except (URLError, TimeoutError, OSError):
            pass
        finally:
            elapsed = (time.perf_counter() - start) * 1000
            with lock:
                results.append((status, elapsed))
            slots.release()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        start = time.perf_counter()
        planned = math.ceil(rate * duration)
        for index in range(planned):
            due = start + index / rate
            delay = due - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            now = time.perf_counter()
            # Skip missed slots rather than producing a catch-up burst.
            if now >= start + duration or now - due >= 1 / rate:
                skipped += 1
                continue
            if not slots.acquire(blocking=False):
                skipped += 1
                continue
            executor.submit(send)
            started += 1
        remaining = start + duration - time.perf_counter()
        if remaining > 0:
            time.sleep(remaining)
        send_window = time.perf_counter() - start
        # Executor shutdown waits for all in-flight requests to finish.
    counts = Counter(status for status, _ in results)
    latencies = [ms for status, ms in results if status == "200"]
    return {
        "target_rps": rate,
        "actual_sent_rps": round(started / send_window, 2),
        "requests": started,
        "generator_skipped": skipped,
        "statuses": dict(counts),
        "success_percent": round(100 * counts["200"] / started, 2) if started else None,
        "503_percent": round(100 * counts["503"] / started, 2) if started else None,
        "success_latency_ms": {"p50": percentile(latencies, .5),
                               "p95": percentile(latencies, .95),
                               "p99": percentile(latencies, .99)},
    }


def main():
    """Validate load parameters and run each requested arrival-rate stage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000/predict")
    parser.add_argument("--rates", type=float, nargs="+", default=[10, 30, 60, 100])
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--concurrency", type=int, default=128)
    args = parser.parse_args()
    if (not math.isfinite(args.duration) or args.duration <= 0
            or args.concurrency <= 0
            or any(not math.isfinite(rate) or rate <= 0 for rate in args.rates)):
        parser.error("rates, duration and concurrency must be positive and finite")
    for rate in args.rates:
        print(f"Starting {rate:g} req/s for {args.duration:g}s", flush=True)
        print(json.dumps(run_stage(args.url, rate, args.duration, args.concurrency),
                         ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
