#!/usr/bin/env python3

"""
Seismic Session Windows (by Flynn Region)

Reads the "seismic-backfill" topic (from seismic-backfill.py) and groups
earthquakes into sessions, one set per Flynn region. A session is a run of
quakes in the same region where each quake is no more than GAP_SEC after the
one before it. The session closes once the region has been quiet for GAP_SEC.

Filters:
  - drop any quake with magnitude <= MIN_MAG
  - drop sessions with fewer than MIN_EVENTS quakes (low-activity regions)

Each kept session becomes a dict, holding its quakes as a list, and is added
to `closed_sessions`.

Two phases (same as seismic-1h-tumbles.py):
  1. History: replay the whole topic, then close every session that has
     been quiet for at least GAP_SEC (plus grace), measured by wall clock.
  2. Live:    keep consuming, and close sessions as they go quiet.
"""

import os
import json
import logging
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import TopicPartition, OFFSET_BEGINNING
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "seismic-backfill"
MIN_MAG = 3.9          # drop quakes with mag <= this
GAP_SEC = 6 * 3600     # quiet time (no quakes in the region) that ends a session
MIN_EVENTS = 3         # sessions smaller than this count as low activity and are dropped
GRACE_MIN = 30         # extra wait for late EMSC reports before closing a session

# Log to a local file and to the console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("seismic-sessions.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# Kafka app; offsets are never committed, so every run replays the full history
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="seismic-sessions",
    auto_offset_reset="earliest",
    consumer_extra_config={"broker.address.family": "v4", "enable.auto.commit": False},
)

open_sessions = defaultdict(list)  # region -> list of open sessions, each {unid: (ts, mag)}
closed_sessions = []               # final output: list of session summary dicts


# Epoch seconds -> readable UTC string
def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# Time of a session's most recent quake
def last_ts(session):
    return max(t for t, _ in session.values())


# Add a quake to its region. Every open session within GAP_SEC of it is merged
# into one, so a quake that arrives out of order can join two sessions together.
def add_event(region, unid, ts, mag):
    near = [s for s in open_sessions[region]
            if min(t for t, _ in s.values()) - GAP_SEC <= ts <= last_ts(s) + GAP_SEC]
    merged = {}
    for s in near:
        open_sessions[region].remove(s)
        merged.update(s)
    merged[unid] = (ts, mag)  # dedupe by unid, latest report wins
    open_sessions[region].append(merged)


# Turn a session into a summary dict with its quakes as a time-ordered list
def summarize(region, session):
    quakes = sorted(({"time": iso(t), "mag": m, "unid": u} for u, (t, m) in session.items()),
                    key=lambda q: q["time"])
    return {
        "region": region,
        "start": quakes[0]["time"],
        "end": quakes[-1]["time"],
        "count": len(quakes),
        "max_mag": max(q["mag"] for q in quakes),
        "quakes": quakes,
    }


# Close every session that has been quiet past the watermark; keep the
# active ones, drop the low-activity ones
def emit_closed(watermark):
    for region, sessions in open_sessions.items():
        for s in [s for s in sessions if last_ts(s) + GAP_SEC <= watermark]:
            sessions.remove(s)
            if len(s) < MIN_EVENTS:
                continue
            summary = summarize(region, s)
            closed_sessions.append(summary)
            log.info(f"{summary['start']} -> {summary['end']} UTC | {region:32} | "
                     f"n={summary['count']:3d} | max=M{summary['max_mag']:.1f} | "
                     f"mags={[q['mag'] for q in summary['quakes']]}")


# Parse one Kafka message, apply the magnitude filter, add it to a session
def handle(raw):
    try:
        p = json.loads(raw)["data"]["properties"]
        ts = datetime.fromisoformat(p["time"].replace("Z", "+00:00")).timestamp()
        unid, mag, region = p["unid"], p.get("mag"), p["flynn_region"]
    except (KeyError, ValueError, TypeError) as e:
        log.warning(f"skipping malformed message: {e}")
        return False
    if mag is None or mag <= MIN_MAG:
        return False
    add_event(region, unid, ts, mag)
    return True


# Poll once: None = nothing arrived in 1 s, b"" = Kafka error, else the message value
def poll_value(consumer):
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        return None
    if msg.error():
        log.error(f"Kafka error: {msg.error()}")
        return b""
    return msg.value()


# Replay history, close quiet sessions, then stream live until Ctrl-C
def run():
    watermark = lambda: datetime.now(timezone.utc).timestamp() - GRACE_MIN * 60
    try:
        with app.get_consumer() as consumer:
            # Assign every partition at offset 0 directly (no group rebalance, no commits)
            parts = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
            consumer.assign([TopicPartition(TOPIC, p, OFFSET_BEGINNING) for p in parts])
            log.info(f"Replaying '{TOPIC}' ({len(parts)} partitions) on {KAFKA_BROKER}")

            # Phase 1: read until a 1 s poll comes back empty (history exhausted)
            kept = 0
            while (value := poll_value(consumer)) is not None:
                if value:
                    kept += handle(value)
            log.info(f"History: {kept} quakes > M{MIN_MAG} across {len(open_sessions)} regions")
            emit_closed(watermark())
            log.info(f"{len(closed_sessions)} active sessions (>= {MIN_EVENTS} quakes) closed so far")

            # Phase 2: keep consuming; close sessions once their region goes quiet
            log.info(f"--- live: sessions close after {GAP_SEC // 3600}h quiet + {GRACE_MIN}m grace ---")
            while True:
                if value := poll_value(consumer):
                    handle(value)
                emit_closed(watermark())
    except KeyboardInterrupt:
        log.info("Stopped")
    except Exception as e:
        log.exception(f"Error consuming from Kafka: {e}")


if __name__ == "__main__":
    run()
