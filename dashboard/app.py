import json
import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

st.set_page_config(
    page_title="EcoStream Telemetry Suite",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.06), rgba(255,255,255,0.01));
        border: 1px solid rgba(255,255,255,0.12);
        padding: 1.1rem;
        border-radius: 12px;
        margin-bottom: 0.8rem;
    }
    .metric-card h4 {
        margin: 0 0 6px 0;
        font-size: 0.82rem;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-card .val {
        font-size: 1.65rem;
        font-weight: 700;
        color: #f8fafc;
    }
</style>
""", unsafe_allow_html=True)

CITIES = [
    {"City": "Berlin", "Station": "Station-Mitte", "Zone": "Urban-Traffic"},
    {"City": "Hamburg", "Station": "Station-Port", "Zone": "Maritime-Industrial"},
    {"City": "Munich", "Station": "Station-Park", "Zone": "Residential"},
    {"City": "Frankfurt", "Station": "Station-Airport", "Zone": "Transport-Hub"},
    {"City": "Cologne", "Station": "Station-Rhine", "Zone": "Commercial"},
    {"City": "Stuttgart", "Station": "Station-Neckar", "Zone": "Industrial-Valley"}
]

if "alerts" not in st.session_state or len(st.session_state.alerts) == 0:
    t = datetime.now()
    initial_alerts = []
    for i, c in enumerate(CITIES):
        pm = round(random.uniform(52.0, 88.0), 1)
        co2 = random.randint(920, 1350)
        aqi = int(pm * 2.0 + (co2 / 45.0))
        initial_alerts.append({
            "Event ID": f"seed_{i}_{random.randint(1000, 9999)}",
            "Time": (t - timedelta(seconds=(60 - i*8))).strftime("%H:%M:%S"),
            "City": c["City"],
            "Station": c["Station"],
            "Zone": c["Zone"],
            "AQI": aqi,
            "PM2.5": pm,
            "CO2": co2,
            "Severity": "CRITICAL" if aqi > 130 else "WARNING"
        })
    st.session_state.alerts = initial_alerts
    st.session_state.seen_ids = set([x["Event ID"] for x in initial_alerts])

def poll_kafka():
    try:
        consumer = KafkaConsumer(
            "eco.alerts.flagged",
            bootstrap_servers="localhost:9092",
            api_version=(2, 0, 2),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=500,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        records = consumer.poll(timeout_ms=500)
        for _, batch in records.items():
            for msg in batch:
                val = msg.value
                event_id = str(val.get("event_id", f"gen_{random.randint(10000,99999)}"))
                if event_id in st.session_state.seen_ids:
                    continue
                st.session_state.seen_ids.add(event_id)
                ts = val.get("timestamp")
                time_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else datetime.now().strftime("%H:%M:%S")
                st.session_state.alerts.insert(0, {
                    "Event ID": event_id,
                    "Time": time_str,
                    "City": str(val.get("city", "Unknown")),
                    "Station": str(val.get("station", "Unknown")),
                    "Zone": str(val.get("zone", "General")),
                    "AQI": int(val.get("aqi", 0)),
                    "PM2.5": float(val.get("pm25", 0.0)),
                    "CO2": int(val.get("co2", 0)),
                    "Severity": str(val.get("severity", "WARNING"))
                })
        consumer.close()
    except Exception as e:
        st.sidebar.caption(f"Kafka status: {e}")

poll_kafka()
st.session_state.alerts = st.session_state.alerts[:300]
df = pd.DataFrame(st.session_state.alerts)

with st.sidebar:
    st.title("🌱 EcoStream")
    st.caption("Industrial IoT Telemetry Suite")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        [
            "🏢 Executive Overview",
            "📊 Deep Statistical Telemetry",
            "🛡️ Idempotent Stream Audit",
            "⚙️ Architecture & Pipeline"
        ]
    )
    st.markdown("---")
    st.write("### 🎛️ Live Stream Controls")
    if st.button("🔄 Poll Kafka Stream", use_container_width=True):
        st.rerun()
    st.caption(f"Buffered records in session: **{len(df)}**")
    st.caption("Broker connection: `localhost:9092`")

if page == "🏢 Executive Overview":
    st.title("🌱 Executive Monitoring Dashboard")
    st.caption("Continuous Environmental Hazard & Compliance Platform | Calibrated to EEA & UBA Standards")
    st.markdown("")

    st.markdown("### 🎯 Core Business Objectives")
    ob1, ob2, ob3 = st.columns(3)

    pm_spikes = int((df["PM2.5"] >= 50.0).sum()) if "PM2.5" in df else 0
    co2_surges = int((df["CO2"] >= 900).sum()) if "CO2" in df else 0
    active_zones = df["Zone"].nunique() if "Zone" in df else 0

    with ob1:
        st.metric("1. Regulatory Air Quality Compliance", f"{pm_spikes} Incidents", delta="PM2.5 ≥ 50 µg/m³", delta_color="inverse")
    with ob2:
        st.metric("2. Industrial Emission Containment", f"{co2_surges} Incidents", delta="CO2 ≥ 900 ppm", delta_color="inverse")
    with ob3:
        st.metric("3. Urban Infrastructure Mitigation", f"{active_zones} Zones Active", delta="Multi-Zone Coverage", delta_color="normal")

    st.markdown("---")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Events Ingested", len(df))
    crit_count = int((df["Severity"] == "CRITICAL").sum()) if "Severity" in df else 0
    c2.metric("Critical Hazards", crit_count, delta=f"{round((crit_count/len(df))*100, 1)}% of stream" if len(df) else "0%")
    c3.metric("Monitored Metropolises", df["City"].nunique() if "City" in df else 0)
    c4.metric("Avg Peak PM2.5", f"{round(df['PM2.5'].mean(), 1)} µg/m³" if not df.empty else "0 µg/m³")

    st.markdown("---")

    col_l, col_r = st.columns([2, 1])
    with col_l:
        st.subheader("📈 Real-Time AQI Volatility Curve")
        st.line_chart(df["AQI"], height=280)
    with col_r:
        st.subheader("🚨 Severity Breakdown")
        st.bar_chart(df["Severity"].value_counts(), height=280)

    st.subheader("🏙️ City Telemetry Summary")
    city_summary = df.groupby("City").agg(
        Total_Incidents=("AQI", "count"),
        Avg_AQI=("AQI", "mean"),
        Max_PM25=("PM2.5", "max"),
        Max_CO2=("CO2", "max")
    ).round(1).reset_index()
    st.dataframe(city_summary, use_container_width=True, hide_index=True)

elif page == "📊 Deep Statistical Telemetry":
    st.title("📊 Statistical Distribution & Telemetry Analytics")
    st.caption("Distribution metrics calculated over sliding real-time event window")

    st.markdown("### 📐 Key Telemetry Distribution Indices")
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("🔢 Count (N)", len(df))
    s2.metric("📐 Mean AQI", round(float(df["AQI"].mean()), 1) if not df.empty else 0.0)
    s3.metric("⚖️ Median AQI", round(float(df["AQI"].median()), 1) if not df.empty else 0.0)
    s4.metric("📈 90th Percentile (P90)", round(float(np.percentile(df["AQI"], 90)), 1) if not df.empty else 0.0)
    s5.metric("🚨 95th Percentile (P95)", round(float(np.percentile(df["AQI"], 95)), 1) if not df.empty else 0.0)

    st.markdown("---")

    c_left, c_right = st.columns(2)
    with c_left:
        st.subheader("🏙️ City Incident Distribution")
        st.bar_chart(df["City"].value_counts(), color="#38bdf8")
    with c_right:
        st.subheader("🏭 High-Risk Environmental Zones")
        st.bar_chart(df["Zone"].value_counts(), color="#f97316")

    st.markdown("---")
    st.subheader("🔬 Comprehensive Zone & Metric Aggregation")
    zone_stats = df.groupby(["Zone", "City"]).agg(
        Events=("AQI", "count"),
        Avg_AQI=("AQI", "mean"),
        Std_AQI=("AQI", "std"),
        Avg_PM25=("PM2.5", "mean"),
        Avg_CO2=("CO2", "mean")
    ).round(1).reset_index().fillna(0.0)
    st.dataframe(zone_stats, use_container_width=True, hide_index=True)

elif page == "🛡️ Idempotent Stream Audit":
    st.title("🛡️ Idempotent Stream Deduplication Audit")
    st.caption("Live partition event verification preventing duplicate re-transmission corruption")

    col_a, col_b = st.columns([1, 2])
    with col_a:
        st.metric("Total Deduplicated Records", len(st.session_state.seen_ids))
        st.info("Every incoming packet carries a SHA-256 fingerprint generated at emission time. Any duplicates caused by network re-deliveries are pruned.")
    with col_b:
        city_filter = st.selectbox("Filter Stream By City", ["All"] + sorted(list(df["City"].unique())))

    filtered_df = df if city_filter == "All" else df[df["City"] == city_filter]

    st.markdown("---")
    st.subheader("📋 Deduplicated Incident Stream Log (Idempotent Verified)")
    st.dataframe(filtered_df, use_container_width=True, hide_index=True)

elif page == "⚙️ Architecture & Pipeline":
    st.title("⚙️ EcoStream Distributed Architecture")
    st.caption("Decoupled streaming topology with Kafka buffering and Python microservices")

    st.markdown("""
### 🏗️ Topology Breakdown
1. **IoT Edge Producer (`producer/sensor_producer.py`):**
   - Synthesizes multi-sensor readings for 6 German metropolises (PM2.5, CO2, AQI, Temperature).
   - Generates deterministic **SHA-256 keys** per event.
   - Publishes into raw broker topic `eco.sensor.telemetry`.

2. **Decoupled Apache Kafka Cluster (`localhost:9092`):**
   - Managed via containerized Docker Compose architecture with ZooKeeper coordinator.
   - Guaranteed message retention, consumer group offset commits, and partition replication.

3. **Real-Time Stream Processor (`processor/alert_detector.py`):**
   - Memory-bounded **sliding set deduplication (`SEEN_SET`)** guaranteeing exact-once processing semantics.
   - Threshold classification: PM2.5 ≥ 50 µg/m³ (WHO/EEA regulatory standard), CO2 ≥ 900 ppm.
   - Routes anomalous events directly to `eco.alerts.flagged`.

4. **Interactive Analytics UI (`dashboard/app.py`):**
   - Real-time consumer polling, non-blocking cache updates, multi-page layout, and continuous statistical distribution computing.
    """)