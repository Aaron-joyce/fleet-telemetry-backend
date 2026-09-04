# System Design

## 1. Adding a New Feature: Does the Current Design Accommodate It?

The short answer is yes, at least for the class of features you'd typically add to a fleet dashboard. The design is layered enough that most additions plug into one place without touching everything else.

Take geofencing as a concrete example — flagging robots that leave a designated zone. Here's where it would land:

**`app/state.py`** is where the robot's record lives, so that's where you'd add the flag. Two new fields on the dict: `in_alert: bool` and `alert_reason: str | None`. The geofence check itself goes inside `update_robot()`, right after the coordinate fields are written — at that point you have both the new position and the boundary, so you evaluate and set the flag atomically inside the existing lock.

**`app/consumer.py`** is the ingest path, and it's already calling `fleet_state.update_robot()` for every frame. If you want an immediate push when a breach happens — rather than waiting for the next broadcast tick — you add a check after the `update_robot()` call and publish an alert event to a separate MQTT topic like `fleet/alerts/{robot_id}`.

**`app/main.py`** gets a new route: `GET /robots/alerts` that just filters `get_all_robots()` for records where `in_alert` is `True`. One function, one decorator.

**`app/websocket.py` needs zero changes.** The `fleet_broadcast_worker` in `app/main.py` serialises whatever fields are currently on each robot dict. The new `in_alert` field appears automatically in the next broadcast — no modifications to the connection manager or the broadcast loop required.

That's what makes the current structure work well for incremental features: state is centralised in one place, the consumer is a thin routing layer, and the broadcast path is generic. The points of extension are obvious and isolated.

---

## 2. Scaling from 8 to 500 Robots: What Breaks First?

The first thing that would visibly degrade is the broadcast loop — specifically the combination of `fleet_broadcast_worker()` in `app/main.py` and `manager.broadcast()` in `app/websocket.py`.

Here's why that specifically, rather than something else. At 8 robots, a full fleet snapshot serialised to JSON is around 1 KB. At 500 robots it's 60–80 KB. The broadcast runs at 2 Hz. If you have 50 dashboard clients connected, you're pushing roughly 7.5 MB/s (around 60 Mbps) of serialised JSON through a single Python process on a single event loop. The `json.dumps()` call in `fleet_broadcast_worker()` runs synchronously, and at that payload size it starts blocking the event loop for several milliseconds per cycle — which delays incoming MQTT message processing and starts causing the ingest pipeline to fall behind.

The lock in `app/state.py` compounds this. At 500 robots publishing at 5 Hz, `update_robot()` is trying to acquire `self._lock` around 2,500 times per second. At the same time, `get_all_robots()` — called on every broadcast tick — holds that same lock while copying 500 dictionaries. Lock contention between the two starts showing up as micro-latencies, and frames get dropped.

Below that, the single `aiomqtt.Client` in `app/consumer.py` iterating over `client.messages` would start to fall behind processing 2,500 messages a second in Python, eventually causing backpressure on the TCP receive buffer back to Mosquitto.

The fix for broadcast at scale is to stop sending full snapshots and switch to differential updates — only push the robots that actually changed since the last tick. That cuts the payload down from 500 records to whatever actually moved, which in a real fleet is usually a fraction of the total.

---

## 3. Operation Under Limited Bandwidth

Three levers to pull, roughly in order of impact:

**Change what you send.** The biggest win is deadband filtering on the publisher side in `robot/publisher.py`. Instead of publishing every frame, a robot only publishes when its position has moved beyond a threshold — say 0.5m in x or y — or when its status changes. When stationary, it sends a minimal keepalive every few seconds rather than a full state frame. This can cut traffic by 80–90% for robots that are idle or charging.

**Change how often you send.** Tie publish frequency to operational state. A robot actively moving through a warehouse warrants 1–2 Hz updates. A robot sitting on a charging dock warrants 0.1 Hz. An error state warrants an immediate event-driven publish regardless of the timer. This lives in the publisher logic in `robot/publisher.py` and doesn't require any backend changes.

**Change how much detail each message carries.** The current JSON frame for a single robot is around 140 bytes — mostly field names repeated as ASCII strings. Switching to a binary format like MessagePack or a packed struct cuts that to around 15 bytes per frame: a 1-byte robot ID, 4 bytes each for x and y, 1 byte for battery, 1 byte for status, 4 bytes for the timestamp. That's roughly a 90% reduction per message. The change touches `robot/publisher.py` on the encode side and `app/consumer.py` on the decode side — the state store and API layer don't care what format the payload arrived in.

---

## 4. A Robot Goes Down Mid-Task

The system finds out through the watchdog in `app/main.py` — `stale_robot_watcher()` runs every second and checks `last_seen_at` for every robot in `FleetState`. That field is updated by `update_robot()` in `app/state.py` on every valid incoming frame using `time.monotonic()`. If a robot goes silent for more than 5 seconds, `check_and_mark_stale()` flips its status to `"offline"`. On the next broadcast tick, `fleet_broadcast_worker()` picks that up and every connected dashboard client sees the updated status.

The obvious improvement here — not yet implemented — would be MQTT Last Will and Testament. You'd configure the robot's MQTT client to register a LWT payload on connect, so if its TCP connection drops unexpectedly the broker publishes it immediately rather than waiting for the 5-second software timeout. That would cut detection latency from up to 5 seconds to near-instant. Right now the watchdog is the only mechanism.

Once the status is `"offline"`, the broadcast loop surfaces that to operators right away. In a real dispatch system, that status transition would also be the trigger to release whatever task the robot was holding and reassign it to the nearest available unit — but that scheduling layer doesn't exist in the current codebase.

---

## 5. Slow or Unreliable Connection: Late, Out-of-Order, and Missing Updates

During a disruption, the backend keeps showing the last known good state for that robot — its last confirmed position and the status it was in when it last reported. Nothing changes for up to 5 seconds. After that, `stale_robot_watcher()` in `app/main.py` marks it as `"offline"` and the dashboard reflects that. The key point is that the offline coordinates don't get silently presented as current — the status field makes it explicit that this data is old.

Out-of-order packets are handled inside `update_robot()` in `app/state.py`. The monotonic timestamp check — `if incoming_t <= current_t: return False` — means that if frame `t=600` arrives before delayed frame `t=590`, the 600 is accepted and written. When 590 eventually shows up, it's silently dropped. The frontend never sees coordinates jump backwards or flicker between two positions.

Recovery is immediate. As soon as the robot reconnects and publishes a fresh frame with a timestamp newer than what's stored, `update_robot()` accepts it and resets `last_seen_at`. The status field is taken directly from whatever the incoming telemetry payload carries — `update_robot()` in `app/state.py` does `telemetry.get("status", current["status"])`, so if the robot sends `"active"` in the reconnect frame that's what gets written. If the payload omits the status field entirely, the previous `"offline"` value persists until the watcher next runs and the robot has been quiet long enough to stay offline — so it's on the publisher to include a status in its first reconnect frame. If the robot buffered messages locally while offline, the right behaviour on the edge side is to discard the intermediate high-frequency frames and only flush the most recent state — sending a burst of 300 offline coordinate updates on reconnect would just trigger 300 monotonic rejections and add unnecessary load without updating anything useful.
