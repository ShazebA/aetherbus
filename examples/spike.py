"""M1 spike — NATS JetStream + CloudEvents roundtrip.

Proves the core wire: publish a CloudEvent to a JetStream subject, subscribe
to the same subject, receive and verify the envelope. No Bus abstraction yet
(that lands in M2). No resolver, no dataref — payload is inline for the spike.

Run:
    docker run --rm -p 4222:4222 nats:latest -js
    python examples/spike.py

Expected output:
    [send] seq=1 id=<uuid>
    [recv] <uuid> type=dev.aetherbus.spike.ping.v1 data={'message': 'hello aetherbus', 'n': 42}
    [ok] roundtrip succeeded (1 event)
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

import nats
from cloudevents.conversion import from_json, to_json
from cloudevents.http import CloudEvent

NATS_URL = "nats://localhost:4222"
STREAM = "AETHERBUS_SPIKE"
SUBJECT = "dev.aetherbus.spike.ping.v1"


async def main() -> None:
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await js.add_stream(name=STREAM, subjects=[SUBJECT])

    received: list[CloudEvent] = []
    done = asyncio.Event()

    async def handler(msg) -> None:
        event = from_json(CloudEvent, msg.data)
        received.append(event)
        print(f"[recv] {event['id']} type={event['type']} data={event.data}")
        await msg.ack()
        done.set()

    psub = await js.subscribe(SUBJECT, cb=handler, durable="spike-consumer")

    event = CloudEvent(
        {
            "id": str(uuid.uuid4()),
            "source": "aetherbus://agent/spike",
            "type": SUBJECT,
            "time": datetime.now(UTC).isoformat(),
            "datacontenttype": "application/json",
        },
        {"message": "hello aetherbus", "n": 42},
    )
    ack = await js.publish(SUBJECT, to_json(event))
    print(f"[send] seq={ack.seq} id={event['id']}")

    await asyncio.wait_for(done.wait(), timeout=5.0)
    assert received and received[0]["id"] == event["id"], "envelope id roundtrip failed"
    print(f"[ok] roundtrip succeeded ({len(received)} event)")

    await psub.unsubscribe()
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
