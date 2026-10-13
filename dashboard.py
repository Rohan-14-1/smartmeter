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

# ── Database Connection ───────────────────────────────────────────────
DB_CONFIG = dict(
    host="localhost",
    user="meter_app",
    password="meter_pass",
    dbname="smartmeter",
)

try:
    conn = psycopg2.connect(**DB_CONFIG)
except psycopg2.Error as err:
    st.error(f"⚠️ Could not connect to PostgreSQL: {err}")
    st.info("Make sure PostgreSQL is running: `brew services start postgresql@14` (or your active version).")
    st.stop()

# ── Check Pipeline Process Status ─────────────────────────────────────
def is_process_running(script_name: str) -> bool:
    try:
        output = subprocess.check_output(["pgrep", "-f", script_name]).decode()
        return len(output.strip().splitlines()) > 0
    except Exception:
        return False

processor_active = is_process_running("processor.py")
producer_active = is_process_running("producer.py")

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
                <span style="color:#94a3b8;">Kafka Producer:</span>
                <span style="color: {'#34d399' if producer_active else '#ef4444'}; font-weight: 600;">
                    {'🟢 Running' if producer_active else '🔴 Stopped'}
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    script_dir = os.path.dirname(os.path.abspath(__file__))

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("▶ Start Stream", use_container_width=True, type="primary"):
            if not processor_active:
                subprocess.Popen(
                    [sys.executable, os.path.join(script_dir, "processor.py")],
                    cwd=script_dir,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                time.sleep(0.5)
            if not producer_active:
                subprocess.Popen(
                    [sys.executable, os.path.join(script_dir, "producer.py"), "--delay", "0.08"],
                    cwd=script_dir,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            st.success("Pipeline started!")
            time.sleep(1)
            st.rerun()

    with col_btn2:
        if st.button("⏹ Stop Stream", use_container_width=True):
            os.system("pkill -f 'processor.py'")
            os.system("pkill -f 'producer.py'")
            st.warning("Stream paused.")
            time.sleep(1)
            st.rerun()

    if st.button("🗑️ Reset Tables to 0", use_container_width=True):
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE meter_readings RESTART IDENTITY;")
        cur.execute("TRUNCATE TABLE meter_metrics;")
        conn.commit()
        cur.close()
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
    st.plotly_chart(fig, use_container_width=True)

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
                use_container_width=True,
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
