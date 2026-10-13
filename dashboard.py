"""
Smart Meter Real-Time Streaming Analytics — Premium Dashboard
Live Kafka -> IsolationForest -> PostgreSQL -> Streamlit Dashboard
"""
import os
import sys
import time
import signal
import warnings
import subprocess
import datetime
import streamlit as st
import pandas as pd
import psycopg2
import plotly.graph_objects as go

warnings.filterwarnings('ignore')

# ── Page Configuration ────────────────────────────────────────────────
st.set_page_config(
    page_title="SmartGrid AI — Live Stream Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Premium Theme & Styling ───────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

    html, body, [class*="css"], .stApp {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        background-color: #0b0f19 !important;
        color: #f1f5f9 !important;
    }

    /* Glassmorphism Containers */
    .glass-card {
        background: linear-gradient(135deg, rgba(23, 32, 54, 0.75) 0%, rgba(15, 23, 42, 0.85) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 20px 24px;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
        backdrop-filter: blur(12px);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .glass-card:hover {
        border-color: rgba(99, 102, 241, 0.3);
        transform: translateY(-2px);
    }

    /* Custom Metric Badges */
    .metric-title {
        font-size: 0.8rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        display: flex;
        align-items: center;
        gap: 6px;
        margin-bottom: 8px;
    }
    .metric-value {
        font-size: 1.85rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        font-family: 'JetBrains Mono', monospace;
    }
    .metric-sub {
        font-size: 0.75rem;
        color: #64748b;
        margin-top: 4px;
    }

    /* Gradient Header */
    .main-title {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(120deg, #38bdf8 0%, #818cf8 50%, #c084fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: -0.03em;
        margin-bottom: 2px;
    }

    /* Pulsing status indicator */
    .live-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 14px;
        background: rgba(16, 185, 129, 0.12);
        border: 1px solid rgba(16, 185, 129, 0.35);
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 600;
        color: #34d399;
    }
    .idle-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 14px;
        background: rgba(245, 158, 11, 0.12);
        border: 1px solid rgba(245, 158, 11, 0.35);
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 600;
        color: #fbbf24;
    }
    .pulse-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background-color: #10b981;
        box-shadow: 0 0 10px #10b981;
        animation: pulse 1.8s infinite;
    }
    .idle-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background-color: #f59e0b;
    }
    @keyframes pulse {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { transform: scale(1.15); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Anomaly Banner */
    .anomaly-banner {
        background: linear-gradient(90deg, rgba(239, 68, 68, 0.18) 0%, rgba(185, 28, 28, 0.08) 100%);
        border: 1px solid rgba(239, 68, 68, 0.4);
        border-radius: 12px;
        padding: 12px 20px;
        margin: 12px 0 20px 0;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }

    /* Buttons */
    .stButton>button {
        border-radius: 10px !important;
        font-weight: 600 !important;
        transition: all 0.2s ease !important;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #080c15 !important;
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }
</style>
""", unsafe_allow_html=True)

import sqlite3
import csv
import threading
import joblib

# ── Database Connection ───────────────────────────────────────────────
def get_db():
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        try:
            if db_url.startswith("postgres://"):
                db_url = db_url.replace("postgres://", "postgresql://", 1)
            return psycopg2.connect(db_url), "postgres"
        except Exception:
            pass
    
    # Try local PostgreSQL
    try:
        DB_CONFIG = dict(
            host=os.environ.get("DB_HOST", "localhost"),
            user=os.environ.get("DB_USER", "meter_app"),
            password=os.environ.get("DB_PASS", "meter_pass"),
            dbname=os.environ.get("DB_NAME", "smartmeter"),
            port=os.environ.get("DB_PORT", "5432"),
            connect_timeout=2
        )
        return psycopg2.connect(**DB_CONFIG), "postgres"
    except Exception:
        # Resilient SQLite fallback (ensures zero crash on Streamlit Cloud)
        sqlite_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "smartmeter_cloud.db")
        c = sqlite3.connect(sqlite_path, check_same_thread=False, timeout=15.0)
        return c, "sqlite"

conn, db_type = get_db()

# Auto-initialize tables
if db_type == "postgres":
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meter_readings (
                reading_pk BIGSERIAL PRIMARY KEY,
                reading_time TIMESTAMP,
                global_active_power NUMERIC(10,3),
                rolling_avg_30 NUMERIC(10,3),
                is_anomaly SMALLINT DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_readings_pk ON meter_readings (reading_pk DESC);
            CREATE TABLE IF NOT EXISTS meter_metrics (
                id INT PRIMARY KEY,
                current_power NUMERIC(10,3),
                rolling_avg NUMERIC(10,3),
                peak_power NUMERIC(10,3),
                total_kwh NUMERIC(12,3),
                anomaly_count INT
            );
        """)
        conn.commit()
else:
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meter_readings (
                reading_pk INTEGER PRIMARY KEY AUTOINCREMENT,
                reading_time TEXT,
                global_active_power REAL,
                rolling_avg_30 REAL,
                is_anomaly INTEGER DEFAULT 0
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meter_metrics (
                id INTEGER PRIMARY KEY,
                current_power REAL,
                rolling_avg REAL,
                peak_power REAL,
                total_kwh REAL,
                anomaly_count INTEGER
            );
        """)

# ── Streaming Control Flag (Thread-Safe & Cross-Session) ─────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
STREAM_FLAG_FILE = os.path.join(BASE_DIR, ".streaming_active")

# Auto-seed initial 60 readings if DB is completely empty (guarantees instant visual dashboard)
try:
    check_cnt = pd.read_sql("SELECT COUNT(*) AS c FROM meter_readings", conn)
    if check_cnt.empty or int(check_cnt.iloc[0]["c"]) == 0:
        csv_file_seed = os.path.join(BASE_DIR, "data.csv")
        if not os.path.exists(csv_file_seed):
            csv_file_seed = os.path.join(BASE_DIR, "data_sample.csv")
        model_p = os.path.join(BASE_DIR, "anomaly_model.pkl")
        seed_model = None
        if os.path.exists(model_p):
            try:
                seed_model = joblib.load(model_p)
            except Exception:
                pass

        from collections import deque
        s_win = deque(maxlen=30)
        s_peak, s_kwh, s_anom = 0.0, 0.0, 0
        with open(csv_file_seed) as f:
            for s_idx, s_r in enumerate(csv.DictReader(f)):
                if s_idx >= 60:
                    break
                p_val = float(s_r["Global_active_power"])
                s_win.append(p_val)
                s_avg = sum(s_win) / len(s_win)
                s_peak = max(s_peak, p_val)
                s_kwh += p_val / 60.0
                if seed_model is not None:
                    try:
                        is_a = int(seed_model.predict([[p_val]])[0] == -1)
                    except Exception:
                        is_a = int(p_val > 4.862)
                else:
                    is_a = int(p_val > 4.862)
                s_anom += is_a
                t_str = s_r.get("Date_Time", "2006-12-16 17:24:00")

                if db_type == "postgres":
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO meter_readings (reading_time, global_active_power, rolling_avg_30, is_anomaly) VALUES (%s,%s,%s,%s)", (t_str, p_val, round(s_avg, 3), is_a))
                        cur.execute("INSERT INTO meter_metrics VALUES (1,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET current_power=EXCLUDED.current_power, rolling_avg=EXCLUDED.rolling_avg, peak_power=EXCLUDED.peak_power, total_kwh=EXCLUDED.total_kwh, anomaly_count=EXCLUDED.anomaly_count", (p_val, round(s_avg, 3), s_peak, round(s_kwh, 3), s_anom))
                        conn.commit()
                else:
                    with conn:
                        conn.execute("INSERT INTO meter_readings (reading_time, global_active_power, rolling_avg_30, is_anomaly) VALUES (?,?,?,?)", (t_str, p_val, round(s_avg, 3), is_a))
                        conn.execute("INSERT OR REPLACE INTO meter_metrics (id, current_power, rolling_avg, peak_power, total_kwh, anomaly_count) VALUES (1,?,?,?,?,?)", (p_val, round(s_avg, 3), s_peak, round(s_kwh, 3), s_anom))

        # Enable streaming by default so cloud visitors see the live curve immediately
        open(STREAM_FLAG_FILE, "w").close()
except Exception:
    pass

# ── Check Pipeline Process Status ─────────────────────────────────────
def is_process_running(script_name: str) -> bool:
    try:
        output = subprocess.check_output(["pgrep", "-f", script_name]).decode()
        return len(output.strip().splitlines()) > 0
    except Exception:
        return False

cloud_stream_on = os.path.exists(STREAM_FLAG_FILE)
processor_active = is_process_running("processor.py") or cloud_stream_on
producer_active = is_process_running("producer.py") or cloud_stream_on

# Background Cloud Streamer Worker (No session_state dependencies)
def run_cloud_streamer():
    csv_file = os.path.join(BASE_DIR, "data.csv")
    if not os.path.exists(csv_file):
        csv_file = os.path.join(BASE_DIR, "data_sample.csv")
    if not os.path.exists(csv_file):
        return

    model_path = os.path.join(BASE_DIR, "anomaly_model.pkl")
    model = None
    if os.path.exists(model_path):
        try:
            model = joblib.load(model_path)
        except Exception:
            pass

    from collections import deque
    window = deque(maxlen=30)
    c_stream, d_type = get_db()

    # Get baseline aggregates
    try:
        curr_m = pd.read_sql("SELECT * FROM meter_metrics WHERE id=1", c_stream)
        if not curr_m.empty:
            peak = float(curr_m.iloc[0]["peak_power"]) if pd.notnull(curr_m.iloc[0]["peak_power"]) else 0.0
            total_kwh = float(curr_m.iloc[0]["total_kwh"]) if pd.notnull(curr_m.iloc[0]["total_kwh"]) else 0.0
            anomaly_count = int(curr_m.iloc[0]["anomaly_count"]) if pd.notnull(curr_m.iloc[0]["anomaly_count"]) else 0
        else:
            peak, total_kwh, anomaly_count = 0.0, 0.0, 0
    except Exception:
        peak, total_kwh, anomaly_count = 0.0, 0.0, 0

    while os.path.exists(STREAM_FLAG_FILE):
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            # Skip first 60 already seeded
            for _ in range(60):
                next(reader, None)

            for row in reader:
                if not os.path.exists(STREAM_FLAG_FILE):
                    break
                try:
                    p = float(row["Global_active_power"])
                except Exception:
                    continue

                window.append(p)
                avg = sum(window) / len(window)
                peak = max(peak, p)
                total_kwh += p / 60.0
                if model is not None:
                    try:
                        is_anom = int(model.predict([[p]])[0] == -1)
                    except Exception:
                        is_anom = int(p > 4.862)
                else:
                    is_anom = int(p > 4.862)
                anomaly_count += is_anom
                t_val = row.get("Date_Time", "2006-12-16 17:24:00")

                if d_type == "postgres":
                    with c_stream.cursor() as cur:
                        cur.execute("INSERT INTO meter_readings (reading_time, global_active_power, rolling_avg_30, is_anomaly) VALUES (%s,%s,%s,%s)", (t_val, p, round(avg, 3), is_anom))
                        cur.execute("INSERT INTO meter_metrics VALUES (1,%s,%s,%s,%s,%s) ON CONFLICT (id) DO UPDATE SET current_power=EXCLUDED.current_power, rolling_avg=EXCLUDED.rolling_avg, peak_power=EXCLUDED.peak_power, total_kwh=EXCLUDED.total_kwh, anomaly_count=EXCLUDED.anomaly_count", (p, round(avg, 3), peak, round(total_kwh, 3), anomaly_count))
                        c_stream.commit()
                else:
                    with c_stream:
                        c_stream.execute("INSERT INTO meter_readings (reading_time, global_active_power, rolling_avg_30, is_anomaly) VALUES (?,?,?,?)", (t_val, p, round(avg, 3), is_anom))
                        c_stream.execute("INSERT OR REPLACE INTO meter_metrics (id, current_power, rolling_avg, peak_power, total_kwh, anomaly_count) VALUES (1,?,?,?,?,?)", (p, round(avg, 3), peak, round(total_kwh, 3), anomaly_count))

                time.sleep(0.25)

# Ensure cloud streamer thread is running if flag is set
if cloud_stream_on and not is_process_running("processor.py"):
    thread_names = [t.name for t in threading.enumerate()]
    if "CloudStreamerThread" not in thread_names:
        t = threading.Thread(target=run_cloud_streamer, name="CloudStreamerThread", daemon=True)
        t.start()

# ── Sidebar Controls ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🎛️ Pipeline Control Center")

    st.markdown(
        f"""
        <div style="background: rgba(255,255,255,0.03); padding: 12px; border-radius: 10px; margin-bottom: 16px; border: 1px solid rgba(255,255,255,0.06);">
            <div style="display:flex; justify-content:space-between; margin-bottom: 6px; font-size: 0.85rem;">
                <span style="color:#94a3b8;">Stream Processor:</span>
                <span style="color: {'#34d399' if processor_active else '#ef4444'}; font-weight: 600;">
                    {'🟢 Running' if processor_active else '🔴 Stopped'}
                </span>
            </div>
            <div style="display:flex; justify-content:space-between; font-size: 0.85rem;">
                <span style="color:#94a3b8;">Data Streamer:</span>
                <span style="color: {'#34d399' if producer_active else '#ef4444'}; font-weight: 600;">
                    {'🟢 Running' if producer_active else '🔴 Stopped'}
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("▶ Start Stream", width="stretch", type="primary"):
            import socket
            kafka_up = False
            try:
                s = socket.create_connection(("127.0.0.1", 9092), timeout=0.8)
                s.close()
                kafka_up = True
            except Exception:
                kafka_up = False

            if kafka_up:
                if not is_process_running("processor.py"):
                    subprocess.Popen(
                        [sys.executable, os.path.join(BASE_DIR, "processor.py")],
                        cwd=BASE_DIR,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    time.sleep(0.5)
                if not is_process_running("producer.py"):
                    subprocess.Popen(
                        [sys.executable, os.path.join(BASE_DIR, "producer.py"), "--delay", "0.08"],
                        cwd=BASE_DIR,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
            else:
                # Set streaming flag and launch background thread
                open(STREAM_FLAG_FILE, "w").close()
                thread_names = [t.name for t in threading.enumerate()]
                if "CloudStreamerThread" not in thread_names:
                    t = threading.Thread(target=run_cloud_streamer, name="CloudStreamerThread", daemon=True)
                    t.start()

            st.success("Pipeline started!")
            time.sleep(0.5)
            st.rerun()

    with col_btn2:
        if st.button("⏹ Stop Stream", width="stretch"):
            if os.path.exists(STREAM_FLAG_FILE):
                try:
                    os.remove(STREAM_FLAG_FILE)
                except Exception:
                    pass
            os.system("pkill -f 'processor.py'")
            os.system("pkill -f 'producer.py'")
            st.warning("Stream paused.")
            time.sleep(0.5)
            st.rerun()

    if st.button("🗑️ Reset Tables to 0", width="stretch"):
        if os.path.exists(STREAM_FLAG_FILE):
            try:
                os.remove(STREAM_FLAG_FILE)
            except Exception:
                pass
        if db_type == "postgres":
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE meter_readings RESTART IDENTITY;")
                cur.execute("TRUNCATE TABLE meter_metrics;")
                conn.commit()
        else:
            with conn:
                conn.execute("DELETE FROM meter_readings;")
                conn.execute("DELETE FROM meter_metrics;")
        st.info("Database reset to 0.")
        time.sleep(0.5)
        st.rerun()

    st.markdown("---")
    st.markdown("#### ⚡ Pipeline Topology")
    st.markdown("""
    - **Source**: UCI Smart Meter CSV
    - **Broker**: Apache Kafka (`smartmeter-events`)
    - **Engine**: Python Rolling Window (30 min)
    - **ML Model**: Scikit-Learn `IsolationForest`
    - **Storage**: PostgreSQL (`smartmeter`)
    - **Refresh**: Live 2-second stream
    """)

    # Telemetry aggregate
    total_df = pd.read_sql(
        "SELECT COUNT(*) AS cnt, COALESCE(SUM(is_anomaly), 0) AS anom FROM meter_readings",
        conn
    )
    total_cnt = int(total_df.cnt[0]) if not total_df.empty else 0
    total_anom = int(total_df.anom[0]) if not total_df.empty else 0
    anom_pct = (100.0 * total_anom / max(1, total_cnt)) if total_cnt > 0 else 0.0

    st.markdown("---")
    st.markdown("#### 📊 Overall Statistics")
    st.metric("Total Streamed", f"{total_cnt:,}")
    st.metric("Total Anomalies", f"{total_anom:,}")
    st.metric("Anomaly Ratio", f"{anom_pct:.2f}%")


# ── Top Header ────────────────────────────────────────────────────────
head_col1, head_col2 = st.columns([3, 1])

with head_col1:
    st.markdown('<div class="main-title">⚡ Smart Meter Streaming Analytics</div>', unsafe_allow_html=True)
    st.markdown(
        '<p style="color: #94a3b8; font-size: 0.95rem; margin-top: -4px;">'
        'Real-time anomaly detection & rolling power consumption analytics with Isolation Forest'
        '</p>',
        unsafe_allow_html=True
    )

with head_col2:
    if processor_active or producer_active:
        st.markdown(
            '<div style="text-align: right; margin-top: 10px;">'
            '<span class="live-badge"><span class="pulse-dot"></span> STREAM ACTIVE (2s)</span>'
            '</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div style="text-align: right; margin-top: 10px;">'
            '<span class="idle-badge"><span class="idle-dot"></span> STREAM IDLE</span>'
            '</div>',
            unsafe_allow_html=True
        )


# ── Fetch Live Metrics ────────────────────────────────────────────────
metrics_df = pd.read_sql("SELECT * FROM meter_metrics WHERE id = 1", conn)

if not metrics_df.empty:
    m = metrics_df.iloc[0]
    p_curr = float(m.current_power)
    p_avg = float(m.rolling_avg)
    p_peak = float(m.peak_power)
    p_kwh = float(m.total_kwh)
    p_anom = int(m.anomaly_count)

    # ── KPI Metric Cards ──────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        st.markdown(f"""
        <div class="glass-card">
            <div class="metric-title">⚡ Current Power</div>
            <div class="metric-value" style="color: #38bdf8;">{p_curr:.3f} <span style="font-size:1rem;color:#64748b;">kW</span></div>
            <div class="metric-sub">Instantaneous draw</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="glass-card">
            <div class="metric-title">📊 30-Pt Average</div>
            <div class="metric-value" style="color: #a78bfa;">{p_avg:.3f} <span style="font-size:1rem;color:#64748b;">kW</span></div>
            <div class="metric-sub">Moving window baseline</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="glass-card">
            <div class="metric-title">🔺 Peak Power</div>
            <div class="metric-value" style="color: #fbbf24;">{p_peak:.3f} <span style="font-size:1rem;color:#64748b;">kW</span></div>
            <div class="metric-sub">Session highest peak</div>
        </div>
        """, unsafe_allow_html=True)

    with c4:
        st.markdown(f"""
        <div class="glass-card">
            <div class="metric-title">🔋 Total Energy</div>
            <div class="metric-value" style="color: #34d399;">{p_kwh:.2f} <span style="font-size:1rem;color:#64748b;">kWh</span></div>
            <div class="metric-sub">Accumulated consumption</div>
        </div>
        """, unsafe_allow_html=True)

    with c5:
        st.markdown(f"""
        <div class="glass-card" style="border-color: rgba(239, 68, 68, {'0.6' if p_anom > 0 else '0.1'});">
            <div class="metric-title">🚨 Anomalies</div>
            <div class="metric-value" style="color: #f87171;">{p_anom}</div>
            <div class="metric-sub">Isolation Forest flags</div>
        </div>
        """, unsafe_allow_html=True)

else:
    st.info("💡 **Pipeline is ready!** Click **'▶ Start Stream'** in the sidebar or run `python3 processor.py` & `python3 producer.py` in your terminal to begin streaming data.")

# ── Recent Readings & Visualization ──────────────────────────────────
recent_df = pd.read_sql(
    "SELECT reading_pk, reading_time, global_active_power, rolling_avg_30, is_anomaly "
    "FROM meter_readings ORDER BY reading_pk DESC LIMIT 150",
    conn
)

if not recent_df.empty:
    recent_df = recent_df.sort_values("reading_pk").reset_index(drop=True)
    recent_df["reading_time"] = pd.to_datetime(recent_df["reading_time"])

    # Split anomalies
    anomalies_df = recent_df[recent_df["is_anomaly"] == 1]

    # Alert if latest reading is an anomaly
    latest_reading = recent_df.iloc[-1]
    if latest_reading["is_anomaly"] == 1:
        st.markdown(f"""
        <div class="anomaly-banner">
            <div>
                <strong style="color: #f87171; font-size: 1.05rem;">🚨 CRITICAL POWER SPIKE DETECTED</strong><br>
                <span style="color: #cbd5e1; font-size: 0.9rem;">
                    Reading at <b>{latest_reading['reading_time'].strftime('%H:%M:%S')}</b> registered 
                    <b>{latest_reading['global_active_power']:.3f} kW</b> (Baseline: {latest_reading['rolling_avg_30']:.3f} kW)
                </span>
            </div>
            <span style="background: #ef4444; color: white; padding: 4px 10px; border-radius: 6px; font-weight: 700; font-size: 0.8rem;">ANOMALY</span>
        </div>
        """, unsafe_allow_html=True)

    # ── Interactive Plotly Chart ──────────────────────────────────────
    fig = go.Figure()

    # Power Line
    fig.add_trace(go.Scatter(
        x=recent_df["reading_time"],
        y=recent_df["global_active_power"],
        mode="lines",
        name="Global Active Power (kW)",
        line=dict(color="#38bdf8", width=2),
        hovertemplate="<b>Power:</b> %{y:.3f} kW<br><b>Time:</b> %{x|%Y-%m-%d %H:%M:%S}<extra></extra>"
    ))

    # Rolling Average
    fig.add_trace(go.Scatter(
        x=recent_df["reading_time"],
        y=recent_df["rolling_avg_30"],
        mode="lines",
        name="30-pt Rolling Average",
        line=dict(color="#f59e0b", width=2, dash="dot"),
        hovertemplate="<b>30-pt Avg:</b> %{y:.3f} kW<br><b>Time:</b> %{x|%Y-%m-%d %H:%M:%S}<extra></extra>"
    ))

    # Anomaly Markers (Glow Red Diamonds)
    if not anomalies_df.empty:
        fig.add_trace(go.Scatter(
            x=anomalies_df["reading_time"],
            y=anomalies_df["global_active_power"],
            mode="markers",
            name="Anomaly (Isolation Forest)",
            marker=dict(
                color="#ef4444",
                size=11,
                symbol="diamond",
                line=dict(color="#ffffff", width=1.5)
            ),
            hovertemplate="<b style='color:#ef4444;'>🚨 ANOMALY SPIKE:</b> %{y:.3f} kW<br><b>Time:</b> %{x|%Y-%m-%d %H:%M:%S}<extra></extra>"
        ))

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(15, 23, 42, 0.4)",
        plot_bgcolor="rgba(11, 15, 25, 0.6)",
        margin=dict(l=20, r=20, t=30, b=20),
        height=450,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=12, color="#94a3b8")
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(255, 255, 255, 0.05)",
            title=None,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(255, 255, 255, 0.05)",
            title="Power (kW)",
            title_font=dict(color="#94a3b8"),
        ),
        hovermode="x unified",
    )

    st.markdown("### 📈 Real-Time Power Telemetry Stream")
    st.plotly_chart(fig, width="stretch")

    # ── Lower Split: Recent Anomalies Table & Summary ─────────────────
    row2_col1, row2_col2 = st.columns([3, 2])

    with row2_col1:
        st.markdown("#### 🚨 Detected Anomaly Log")
        if not anomalies_df.empty:
            display_anom = anomalies_df.tail(8).copy()
            display_anom["Deviation"] = display_anom["global_active_power"] - display_anom["rolling_avg_30"]
            display_anom["Formatted Time"] = display_anom["reading_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

            st.dataframe(
                display_anom[["Formatted Time", "global_active_power", "rolling_avg_30", "Deviation"]]
                .rename(columns={
                    "Formatted Time": "Timestamp",
                    "global_active_power": "Spike Power (kW)",
                    "rolling_avg_30": "Rolling Avg (kW)",
                    "Deviation": "Surge Delta (kW)"
                }),
                width="stretch",
                hide_index=True
            )
        else:
            st.success("✅ No anomalous power spikes detected in the current window.")

    with row2_col2:
        st.markdown("#### 🔍 Isolation Forest Model Insights")
        st.markdown(f"""
        - **Algorithm**: `sklearn.ensemble.IsolationForest`
        - **Contamination**: `1.0%` (0.01)
        - **Estimators**: `200` trees
        - **Current Window Size**: `30 readings`
        - **Recent Anomalies in View**: `{len(anomalies_df)}`
        - **Max Detected Power in View**: `{recent_df['global_active_power'].max():.3f} kW`
        """)

conn.close()

# ── Auto-refresh Interval ─────────────────────────────────────────────
time.sleep(2)
st.rerun()
