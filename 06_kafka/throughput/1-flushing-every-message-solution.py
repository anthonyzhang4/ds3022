"""
SOLUTION 1: Don't flush after every message.

produce() only drops the message in a local buffer; the Kafka client batches and sends
it in the background. Removing the per-message flush() lets the loop keep reading the
WebSocket while delivery happens in parallel. The producer flushes once on exit
(leaving the `with` block), so nothing buffered is lost.

Watch:  `lag` should stay near 0 and `missed` at 0.
"""
import asyncio
import json
import logging
import os
import time
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

# Log to a local file next to this script (and to the console)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(os.path.splitext(__file__)[0] + ".log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

app = Application(broker_address=KAFKA_BROKER, producer_extra_config={"broker.address.family": "v4"})
topic = app.topic(name="trades", value_serializer="json")


async def main():
    """Read trades from the WebSocket and produce each one to Kafka without blocking."""
    count, last_seq, missed = 0, None, 0
    with app.get_producer() as producer:                # exiting the block flushes once
        try:
            async with websockets.connect(URL) as ws:
                async for message in ws:
                    data = json.loads(message)
                    msg = topic.serialize(key=data["symbol"], value=data)
                    producer.produce(topic=topic.name, key=msg.key, value=msg.value)  # buffered, no wait

                    # Bookkeeping: count gaps in `seq` and how far behind real time we are
                    if last_seq is not None and data["seq"] != last_seq + 1:
                        missed += data["seq"] - last_seq - 1
                    last_seq, count = data["seq"], count + 1
                    if count % 1000 == 0:
                        log.info(f"produced={count} missed={missed} lag={time.time() - data['ts']:.2f}s")
        except Exception as e:
            log.error(f"Connection lost: {e!r}")
    log.info(f"Total produced: {count}, missed: {missed}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
