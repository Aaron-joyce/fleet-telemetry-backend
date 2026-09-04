# Answers

## 1. What holds the fleet's current state in your backend, and why that shape, given it has to serve both the WebSocket stream and the polling endpoint consistently?

The state lives in `app/state.py` as a single `FleetState` instance — a Python dictionary keyed by `robot_id`, wrapped in an `asyncio.Lock`. Both the REST handlers and the WebSocket loop in `app/main.py` read from the same object, so there's no sync problem between them: whatever state the consumer last wrote is exactly what both endpoints see.

The dictionary shape is simple on purpose. Each entry holds the fields that actually matter — `robot_id`, `x`, `y`, `battery`, `status`, `t`, and `last_seen_at` — and nothing else. Keying by `robot_id` means `GET /robots/{robot_id}` is a single hash lookup rather than a scan, and building the full fleet snapshot for `GET /robots` or the WebSocket frame is just iterating the values.

The one design detail that earns its complexity is copy-on-read. `get_all_robots()` and `get_robot()` both return shallow copies (`dict(robot)`) rather than references into the internal store. That means the lock is held only for the duration of the copy — a fraction of a millisecond — and callers can do whatever they want with the returned data without risking a `RuntimeError` from a concurrent write mutating the dict mid-serialisation. The `update_robot()` method also runs its monotonic timestamp check inside the same lock: if `incoming_t <= current_t`, the update is discarded immediately. That single guard handles both duplicate QoS 1 redeliveries and genuine out-of-order packets.

---

## 2. Name one real tradeoff you made: the mechanism you chose for robots to reach your backend, its delivery guarantees, and how you reconcile that mechanism's semantics with your WebSocket fanout. Argue for the decision, including its cost.

The mechanism is MQTT with QoS 1. Each simulated robot in `robot/publisher.py` publishes its telemetry to `fleet/telemetry/{robot_id}` with `qos=1`, and the backend consumer in `app/consumer.py` subscribes to `fleet/telemetry/+` at the same QoS level. QoS 1 means the broker guarantees delivery at least once — the publisher retries until it gets a `PUBACK`. That gives confidence that no frame silently disappears in transit, which matters when a robot is reporting a low battery or an error state.

The tension with WebSocket fanout is real though. QoS 1 allows duplicates — if an acknowledgement is delayed, the broker can redeliver the same frame. Forwarding every raw MQTT message directly to WebSocket clients would mean clients occasionally seeing the same update twice, or seeing coordinates flicker if an old retransmitted packet lands after a newer one. Neither is acceptable.

The reconciliation happens in two places. First, `update_robot()` in `app/state.py` gates every incoming frame against the current timestamp: if `incoming_t <= current_t`, it returns `False` and the frame is thrown away, no matter how it got there. Second, the WebSocket endpoint in `app/main.py` doesn't forward individual MQTT events at all — it runs its own independent loop that samples the current state every 500ms and broadcasts a snapshot. So even if ten frames arrive in half a second, the WebSocket client sees one clean, consolidated update. The two concerns — ingest reliability and broadcast clarity — are fully decoupled.

The cost of this is twofold. QoS 1 generates more broker traffic than QoS 0 because of the `PUBACK` round trips on every publish. And the 500ms polling interval means the WebSocket stream lags slightly behind real events — if a robot's battery hits zero between ticks, the frontend won't know for up to half a second. For a fleet operations dashboard that's fine. For something requiring millisecond-level reaction, you'd want an event-driven push rather than a polling loop.

---

## 3. What did you leave out, and what would you build next given more time?

A few things were consciously skipped given the scope.

There's no persistence layer. `FleetState` lives entirely in process memory, so a container restart wipes everything. There's no record of where a robot has been, how its battery degraded over a shift, or what its status was at any given point in the past. The stretch goal endpoint — `GET /robots/history/{robot_id}` — was left out for exactly this reason: without a database behind it, it would have nothing to query.

Security is also absent at the broker level. Mosquitto is configured with `allow_anonymous true` on plaintext port 1883. Any process on the same network can publish to `fleet/telemetry/+` and inject bogus state.

The WebSocket side is actually more solid than it might look. The endpoint in `app/main.py` delegates entirely to a `ConnectionManager` in `app/websocket.py` — it tracks all active connections in a locked set, and a single shared `fleet_broadcast_worker` task (also in `app/main.py`) polls state every 500ms and calls `manager.broadcast()` once for everyone. The broadcast uses `asyncio.gather` with a 1-second `asyncio.wait_for` timeout per client, so a slow or dead connection gets dropped without blocking the others. That said, there's still no backpressure beyond the timeout — if clients are genuinely slow consumers rather than just dead, they'll hit the timeout and get disconnected rather than receiving a degraded-quality stream.

Given more time, the thing I'd prioritise first is a time-series store — SQLite works fine locally, TimescaleDB or ClickHouse if scale is a concern — wired into the consumer in `app/consumer.py` so every incoming frame is persisted as it arrives. That alone unlocks the history endpoint, fleet playback, and trend analysis on battery or task throughput. After that I'd look at broker authentication: per-robot client certificates on Mosquitto so a robot can only publish to its own topic, not the whole `fleet/telemetry/+` namespace.
