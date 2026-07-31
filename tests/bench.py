import asyncio
import time
from novabus import Bus

N = 200_000


def run(high_rate: bool):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bus = Bus(loop=loop)
    bus.mark_critical("TradingSignal", "interpret_trading_signal")
    bus.set_high_rate_mode(high_rate)

    received = {"count": 0}
    bus.subscribe("bench", lambda *a: received.__setitem__("count", received["count"] + 1))
    bus.start()

    async def driver():
        t0 = time.perf_counter()
        for i in range(N):
            bus.publish("Bench", "tick", i, {"i": i})
        publish_done = time.perf_counter()

        deadline = time.perf_counter() + 30
        while received["count"] < N and time.perf_counter() < deadline:
            await asyncio.sleep(0.001)
        t1 = time.perf_counter()
        return t0, publish_done, t1

    t0, publish_done, t1 = loop.run_until_complete(driver())
    bus.stop()
    if bus._task:
        loop.run_until_complete(asyncio.gather(bus._task, return_exceptions=True))
    loop.close()

    publish_elapsed = publish_done - t0
    total_elapsed = t1 - t0
    print(f"high_rate_mode={high_rate}")
    print(f"  events published:      {N}")
    print(f"  events received:       {received['count']}")
    print(f"  publish-only:          {publish_elapsed:.3f}s -> {N/publish_elapsed:,.0f} events/sec")
    print(f"  end-to-end drain:      {total_elapsed:.3f}s -> {received['count']/total_elapsed:,.0f} events/sec")
    print()


if __name__ == "__main__":
    run(high_rate=False)
    run(high_rate=True)
