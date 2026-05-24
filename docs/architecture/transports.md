# Transports

`Bus(Settings(transport_type="kafka", ...))` is a drop-in replacement for the NATS default. The conformance test suite parametrizes every integration scenario over both transports, so semantic drift between them is caught at CI time, not in production.

## Semantic translation table

| AetherBus concept | NATS JetStream | Kafka |
|---|---|---|
| Topic name `dev.aetherbus.research.finding.v1` | Subject (dots are tokens) | Topic (pass-through; dots are valid Kafka topic chars) |
| Stream container | JetStream stream | (no equivalent — topics are the unit) |
| `ensure_stream(name, subjects)` | `add_stream(name, subjects)` | `AIOKafkaAdminClient.create_topics` for concrete subjects; wildcards skipped |
| `queue_group="workers"` | `deliver_group` + shared durable | Consumer `group_id` |
| `replay_from="earliest"` | `DeliverPolicy.ALL` | `auto_offset_reset="earliest"` |
| `replay_from="latest"` | `DeliverPolicy.NEW` | `auto_offset_reset="latest"` |
| `replay_from=datetime(...)` | `opt_start_time` | `offsets_for_times` + `seek` |
| `ack()` | `msg.ack()` | `commit({tp: offset+1})` |
| `nack()` | `msg.nak()` | `seek(tp, offset)` (re-poll) |
| `max_in_flight` | `max_ack_pending` | `asyncio.Semaphore` around dispatcher |
| `bus.replay(topic, ...)` | Ephemeral durable pull consumer + `stream.last_seq` snapshot | Fresh consumer group + `end_offsets` snapshot |
| Wildcards | `*` (single token), `>` (rest) | Regex via `consumer.subscribe(pattern=...)` |

## Topic naming

`<domain>.<entity>.<action>.<version>` — e.g. `dev.aetherbus.research.finding.v1`.

Both NATS subjects and Kafka topics accept the same character set, so there's no escaping layer. The CLI normalizes wildcards (`*` vs regex) across transports.

## Choosing one

- **NATS JetStream** is the default. Single Go binary, ~15 MB. Native multi-tenancy via accounts. Native queue groups. The wedge for P1 (solo agent-tinkerer) per [ADR-001](adrs/0001-nats-jetstream-as-default-transport.md).
- **Kafka** is the integration path for teams that already run Kafka. Same `Bus` API. The conformance suite proves semantic parity at CI time.

If you're starting fresh, use NATS. If you're integrating into an existing Kafka shop, switch transports without touching agent code:

```python
# NATS (default)
Bus(Settings.from_env())

# Kafka
Bus(Settings(transport_type="kafka", transport_url="localhost:9092"))
```

## Replay correctness — the snapshot rule

`bus.replay()` walks broker history. Without care, replaying *into the same broker* (the common dev sandbox) becomes a feedback loop: each yielded event triggers a re-publish, which the pull consumer sees as new history.

Both transports terminate replay at an **end-of-history snapshot** captured before iteration starts:

- NATS: `stream.state.last_seq`. Iteration stops when a yielded message's sequence number meets or exceeds the snapshot.
- Kafka: `end_offsets` per partition. Iteration stops when every partition's position reaches its snapshot.

The conformance suite includes `test_replay_into_same_broker_terminates` which proves both transports cap at the expected event count.
