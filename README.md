# novabus

A dependency-free, in-process, priority-aware event bus for Python asyncio apps.

No PyQt5, no Redis, no broker to run. Pure stdlib (`asyncio` + `collections`).

```bash
pip install novabus
```

## Usage

```python
import asyncio
from novabus import Bus

loop = asyncio.get_event_loop()
bus = Bus(loop=loop)

# mark which (source, function_name) pairs jump the queue
bus.mark_critical("TradingSignal", "interpret_trading_signal")

def on_event(source, function_name, result, params):
    print(source, function_name, result, params)

bus.subscribe("my_module", on_event, sources=["TradingSignal"])  # or sources=None for all
bus.start()

bus.publish("TradingSignal", "interpret_trading_signal", result={"side": "buy"}, params={})
```

For maximum throughput when ordering/priority don't matter, call
`bus.set_high_rate_mode(True)` — publish then dispatches synchronously,
bypassing the queue.

## Design notes

- Two priority lanes (critical / non-critical); critical always drains first
  and pauses non-critical delivery until caught up.
- Per-event dedup via a monotonic counter (bounded memory, periodically evicted).
- Stale-event flushing: events older than `stale_after` seconds are dropped
  instead of being delivered late.
- Backpressure: queue has a max size; oldest non-critical events are dropped
  first when full.
- Subscribers are plain callables, optionally filtered by source/function name.

## Benchmark

`python tests/bench.py` publishes 200k events and measures end-to-end delivery.
On a typical dev machine: **~245k events/sec** in normal (priority-queue)
mode, **~800k events/sec** in `high_rate_mode`.

## Links

- Source: https://github.com/addagirisr-dev/novabus
- PyPI: https://pypi.org/project/novabus/
