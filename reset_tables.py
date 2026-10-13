"""
Helper: truncate both tables so the demo starts from zero.

Usage:
    python reset_tables.py
"""
import psycopg2

DB_CONFIG = dict(
    host="localhost",
    user="meter_app",
    password="meter_pass",
    dbname="smartmeter",
)

db = psycopg2.connect(**DB_CONFIG)
db.autocommit = True
cur = db.cursor()
cur.execute("TRUNCATE TABLE meter_readings RESTART IDENTITY")
cur.execute("TRUNCATE TABLE meter_metrics")
print("✅  Both tables truncated — ready for a fresh demo.")
cur.close()
db.close()
