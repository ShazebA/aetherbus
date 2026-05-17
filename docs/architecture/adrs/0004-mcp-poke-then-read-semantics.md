# ADR-004: MCP adapter uses poke-then-read, not push

- **Status:** Accepted
- **Date:** 2026-05-17
- **Deciders:** Shazeb
- **PRD anchor:** §6.4, §9 Q5, §7.2 ADR-004

## Context

The AetherBus MCP server adapter (`aetherbus-mcp serve`) bridges bus topics to MCP resources so any MCP-capable client — Claude Desktop, Cursor, Cline, Goose — can consume the stream without AetherBus-specific code. The adapter could deliver content to the client two ways:

1. **Push.** On every published event, send the resolved payload to the client as part of the notification.
2. **Poke-then-read.** On every event, send `notifications/resources/updated` (URI only, no body); the client decides when to call `resources/read` to fetch the content.

The MCP spec defines both `notifications/resources/updated` and `resources/read` but is non-prescriptive about which to use when. Anthropic's reference servers and the discussion in MCP issue #301 favor poke-then-read; nothing in the spec forbids push.

## Decision

**The adapter emits `notifications/resources/updated` on every published event and never pushes the body.** Clients pull via `resources/read`. The latest event per topic is held in memory; older events are not retained by the adapter (the bus keeps history — call `bus.replay` from M5+).

## Rationale

- **Spec alignment.** Poke-then-read is what `resources/subscribe` is designed for. We picked MCP precisely because of this primitive (PRD §2.3 — "MCP has the primitive but nobody uses it"). Implementing push would route around the spec's intent.
- **Client controls the rate.** A research subagent publishing every 50 ms shouldn't drown the client. The client chooses how often to read; if a user isn't looking at the Claude Desktop sidebar, no payload moves.
- **Privacy / battery.** Pushing a payload means the client receives content it didn't ask for, often over an OS pipe (stdio) or HTTP (SSE). Poke-then-read defers that traffic to user-driven moments.
- **Resource size.** Per design principle P2 (pointers by default), event payloads can be hundreds of KB once resolved. Streaming all of them through the MCP transport is the opposite of what AetherBus is for.
- **Backpressure.** A client that's slow to `resources/read` won't congest the publish path — the adapter just overwrites its `latest` slot for that topic. Push would build a queue we'd have to bound somehow.

## Consequences

- **+** Adapter has no per-client buffering policy to design. The bus is the buffer.
- **+** Clients that don't care about a topic incur zero data cost.
- **+** Late-attaching clients see the latest state on first read, no replay protocol needed at the MCP layer.
- **−** "Latest event per topic" semantics mean a client that polls slowly misses intermediate events. If they need every event they call `bus.replay` directly (M5+) — the MCP surface is intentionally summary-oriented.
- **−** Bursty topics still send one `resources/updated` per event. The client decides whether to coalesce. Most do.

## Alternatives revisited

- **Push the body.** Rejected for the reasons above.
- **Push a diff.** Rejected — no general "diff" for arbitrary CloudEvents payloads, and computing one defeats the point of being a thin convention layer (P1).
- **Coalesce notifications on the server side.** Tempting (debounce N ms) but adds a tuning knob with no obvious default. The client already gets a single URI per notification — coalescing happens at the JSON-RPC transport layer for free.

## Implementation notes

- Built on the low-level `mcp.server.Server` rather than `FastMCP` because the high-level class doesn't expose the active `ServerSession` for unsolicited notifications. We mirror `Server.run` (≈25 LOC) to capture the session — see `aetherbus.mcp_adapter.server.AetherBusMCPServer._run_message_loop`. This couples us to `mcp.server.lowlevel.Server._handle_message` (single-underscore, technically internal); pinned to `mcp>=1.0`. Move to a public hook the moment the SDK offers one.
- The resource URI scheme is `aetherbus://<topic>`. Reserved at our own org so it won't collide with the MCP spec's own URI schemes (`file://`, `http://`, etc.) or any other server's. Per PRD §9 Q5 future iteration: a separate `aetherbus.envelope://` resource type to expose the raw envelope for tools that want it.

## References

- PRD §2.3 (the MCP `resources/subscribe` gap that motivated AetherBus)
- PRD §6.4 (MCP adapter spec)
- PRD §9 Q5 (envelope-vs-content question)
- [MCP spec — resources](https://modelcontextprotocol.io/specification/2025-03-26/server/resources)
- [MCP discussion #301](https://github.com/modelcontextprotocol/specification/discussions/301) — community framing of the same trade-off
