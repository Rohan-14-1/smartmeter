"""
Stream processor — consumes events from Kafka, computes a 30-reading
rolling average, tracks peak power & total energy, runs the IsolationForest
anomaly detector, and writes everything to PostgreSQL.

Usage:
    python processor.py
"""
import json
import os
import joblib
import psycopg2
from collections import deque
from kafka import KafkaConsumer

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(DATA_DIR, "anomaly_model.pkl")

# ── Kafka ────────────────────────────────────────────────────────────
KAFKA_BROKER = "127.0.0.1:9092"
TOPIC = "smartmeter-events"

# ── PostgreSQL ───────────────────────────────────────────────────────
DB_CONFIG = dict(
    host="localhost",
    user="meter_app",
    password="meter_pass",
    dbname="smartmeter",
)


def main():
    # Load the pre-trained anomaly model
    print("📦  Loading anomaly model …")
    model = joblib.load(MODEL_PATH)

    # Connect to Kafka
    print(f"📡  Connecting to Kafka ({KAFKA_BROKER}) …")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="smartmeter-processor",
    )

    # Connect to PostgreSQL
    print("🗄️   Connecting to PostgreSQL …")
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        db = psycopg2.connect(db_url)
    else:
        db = psycopg2.connect(**DB_CONFIG)
    db.autocommit = True
    cur = db.cursor()
    cur.execute("SET datestyle = 'DMY';")

    # Rolling-window state
    window: deque = deque(maxlen=30)
    peak: float = 0.0
    total_kwh: float = 0.0
    anomaly_count: int = 0
    processed: int = 0

    print("⏳  Waiting for messages …\n")

    for msg in consumer:
        event = msg.value
        power = event["global_active_power"]

        # ── Rolling average ──────────────────────────────────────────
        window.append(power)
        rolling_avg = sum(window) / len(window)

        # ── Peak & energy ────────────────────────────────────────────
        peak = max(peak, power)
        total_kwh += power / 60  # kW for 1 minute → kWh

        # ── Anomaly detection ────────────────────────────────────────
        is_anomaly = int(model.predict([[power]])[0] == -1)
        anomaly_count += is_anomaly

        # ── Write to PostgreSQL ──────────────────────────────────────
        cur.execute(
            "INSERT INTO meter_readings "
            "(reading_time, global_active_power, rolling_avg_30, is_anomaly) "
            "VALUES (%s, %s, %s, %s)",
            (event["reading_time"], power, round(rolling_avg, 3), is_anomaly),
        )
        cur.execute(
            "INSERT INTO meter_metrics (id, current_power, rolling_avg, peak_power, total_kwh, anomaly_count) "
            "VALUES (1, %s, %s, %s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET "
            "current_power = EXCLUDED.current_power, "
            "rolling_avg = EXCLUDED.rolling_avg, "
            "peak_power = EXCLUDED.peak_power, "
            "total_kwh = EXCLUDED.total_kwh, "
            "anomaly_count = EXCLUDED.anomaly_count",
            (power, round(rolling_avg, 3), peak, round(total_kwh, 3), anomaly_count),
        )

        processed += 1
        if processed % 50 == 0:
            anom_flag = " 🔴 ANOMALY!" if is_anomaly else ""
            print(
                f"   #{processed:>6,}  |  power={power:.3f} kW  "
                f"avg={rolling_avg:.3f}  peak={peak:.3f}  "
                f"anomalies={anomaly_count}{anom_flag}"
            )


if __name__ == "__main__":
    main()
