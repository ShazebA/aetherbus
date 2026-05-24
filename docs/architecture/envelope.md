# Envelope

Every AetherBus event is a CloudEvents 1.0 envelope serialized as a single JSON document (structured mode, see [ADR-002](adrs/0002-cloudevents-structured-envelope.md)).

## Required attributes (CloudEvents 1.0)

| Field | Type | Meaning |
|---|---|---|
| `specversion` | string | Always `"1.0"`. |
| `id` | string | UUIDv7 — sortable, time-prefixed. See [ADR-005](adrs/0005-uuidv7-event-ids.md). |
| `source` | URI | Producer identity, e.g. `aetherbus://agent/research-worker-1`. |
| `type` | string | Reverse-DNS topic: `dev.aetherbus.research.finding.v1`. |
| `time` | RFC 3339 | Publish time in UTC. |
| `datacontenttype` | string | `application/json` (default) or `application/octet-stream`. |

## AetherBus extension attributes

Per CloudEvents extension rules, names are lowercase letters/digits only.

| Field | Type | Meaning |
|---|---|---|
| `dataref` | URI string | Pointer to the actual payload. Required when `data` is absent. |
| `aetherbusschema` | URI string | JSON Schema for the resolved payload. |
| `aetherbusttl` | int | Cache hint for resolvers, in seconds. |
| `aetherbusdependencies` | string | Comma-separated event IDs — the provenance DAG. |
| `aetherbuscorrelationid` | string | Workflow / trace ID, OpenTelemetry-compatible. |
| `aetherbussignature` | string (optional) | JWS over the envelope. v2 feature. |

## Either / or — data vs dataref

```python
# Inline (≤4 KB serialized) — fine for small payloads
envelope.data = {"summary": "claim-check works"}

# Dataref — for everything else
envelope.dataref = "vec://chroma/findings/abc-123"
```

Both set raises `EnvelopeError`. Both unset is allowed (heartbeat / tick events).

## On the wire

A representative envelope, pretty-printed:

```json
{
  "specversion": "1.0",
  "id": "0192a4f6-7b1c-7c2e-9c0a-12ab34cd56ef",
  "source": "aetherbus://agent/research-worker-1",
  "type": "dev.aetherbus.research.finding.v1",
  "time": "2026-05-17T15:42:00.123456+00:00",
  "datacontenttype": "application/json",
  "data": {
    "query": "kafka for agents",
    "summary": "claim-check works"
  },
  "aetherbuscorrelationid": "trace-0192a4f6",
  "aetherbusdependencies": "0192a3e1-..."
}
```

## Why structured-JSON and not binary

NATS supports headers, so [binary mode](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md) is technically available. Structured-JSON wins for AetherBus because:

- One serialization across NATS, Kafka, and any future transport.
- The Replay UI and `aetherbus tail` decode every event with one `json.loads`.
- The compaction win from binary evaporates the moment we use `dataref` — which is the default.

See [ADR-002](adrs/0002-cloudevents-structured-envelope.md) for the full rationale.
