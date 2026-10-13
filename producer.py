"""
Kafka producer — replays data.csv rows as streaming events to
the 'smartmeter-events' topic.

Each row is sent as a JSON dict:
  {"reading_time": "...", "global_active_power": float}

Usage:
    python producer.py          # default: 5 readings/sec (0.2 s delay)
    python producer.py --delay 0.1   # 10 readings/sec
"""
import csv
import json
import time
import os
import argparse
from kafka import KafkaProducer

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(DATA_DIR, "data.csv")

KAFKA_BROKER = "127.0.0.1:9092"
TOPIC = "smartmeter-events"


def run(delay: float = 0.2, max_rows: int = 0):
    """
    Read data.csv and publish each row to Kafka.

    Args:
        delay:    seconds between sends (controls replay speed)
        max_rows: stop after this many rows (0 = unlimited)
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    sent = 0
    print(f"🚀  Starting producer  (delay={delay}s, topic={TOPIC})")
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            try:
                power = float(row["Global_active_power"])
            except (ValueError, KeyError):
                continue  # skip blanks / '?' values

            event = {
                "reading_time": row["Date_Time"],
                "global_active_power": power,
            }
            producer.send(TOPIC, value=event)
            sent += 1

            if sent % 100 == 0:
                print(f"   ➜ Sent {sent:,} readings …")

            if 0 < max_rows <= sent:
                break

            time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"\n✅  Done — sent {sent:,} readings.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart-meter Kafka producer")
    parser.add_argument("--delay", type=float, default=0.2,
                        help="Seconds between sends (default 0.2)")
    parser.add_argument("--max-rows", type=int, default=0,
                        help="Max rows to send (0 = unlimited)")
    args = parser.parse_args()
    run(delay=args.delay, max_rows=args.max_rows)
