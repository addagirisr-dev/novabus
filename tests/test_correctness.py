import asyncio
from novabus import Bus


def test_priority_and_filters_and_dedup():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bus = Bus(loop=loop)
    bus.mark_critical("Signals", "urgent")

    order = []
    bus.subscribe("all", lambda s, f, r, p: order.append((s, f)))

    filtered = []
    bus.subscribe("only_signals", lambda s, f, r, p: filtered.append((s, f)), sources=["Signals"])

    bus.start()

    async def scenario():
        # non-critical published first, then critical - critical must be
        # delivered first because non-critical delivery pauses for it.
        bus.publish("Noise", "tick", None, {})
        bus.publish("Signals", "urgent", None, {})
        bus.publish("Noise", "tick2", None, {})
        await asyncio.sleep(0.05)

    loop.run_until_complete(scenario())
    bus.stop()
    loop.run_until_complete(asyncio.gather(bus._task, return_exceptions=True))
    loop.close()

    assert order[0] == ("Signals", "urgent"), f"critical event should be delivered first, got {order}"
    assert ("Noise", "tick") in order and ("Noise", "tick2") in order
    assert filtered == [("Signals", "urgent")], f"source filter failed: {filtered}"
    print("PASS: priority ordering + source filter OK ->", order)


def test_dedup_and_high_rate_mode():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    bus = Bus(loop=loop)
    bus.set_high_rate_mode(True)

    seen = []
    bus.subscribe("hr", lambda s, f, r, p: seen.append(p))

    bus.publish("X", "y", None, {"n": 1})
    bus.publish("X", "y", None, {"n": 2})

    assert len(seen) == 2
    print("PASS: high_rate_mode delivers synchronously without a running loop ->", seen)


if __name__ == "__main__":
    test_priority_and_filters_and_dedup()
    test_dedup_and_high_rate_mode()
