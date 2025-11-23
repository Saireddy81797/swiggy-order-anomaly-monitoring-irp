import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from monitoring_utils import generate_order_data, detect_anomalies
from incident_manager import load_incident_log, log_incidents, INCIDENT_LOG_FILE

st.set_page_config(
    page_title="Swiggy Order Anomaly Monitoring & IRP",
    layout="wide"
)

st.title("🧠 Swiggy-like Real-Time Order Anomaly Monitoring & Incident Response System")
st.caption(
    "Simulated monitoring dashboard for order KPIs, anomaly detection, "
    "incident logging, and incident response plan (IRP)."
)

# ---------------------------
# SIDEBAR CONTROLS
# ---------------------------
st.sidebar.header("⚙️ Simulation & Alert Settings")

minutes = st.sidebar.slider(
    "Simulated time window (minutes)",
    min_value=30,
    max_value=240,
    value=120,
    step=30,
    help="How many past minutes of order data to simulate."
)

order_drop_threshold = st.sidebar.number_input(
    "Alert: Order Volume Drop (%)",
    min_value=10,
    max_value=90,
    value=40,
    step=5,
    help="Trigger incident if orders drop more than this % vs baseline."
)

delivery_delay_threshold = st.sidebar.number_input(
    "Alert: Avg Delivery Time (minutes)",
    min_value=20,
    max_value=120,
    value=50,
    step=5,
    help="Trigger incident if avg delivery time exceeds this."
)

payment_failure_threshold = st.sidebar.number_input(
    "Alert: Payment Failure Rate (%)",
    min_value=1,
    max_value=50,
    value=10,
    step=1,
    help="Trigger incident if payment failure rate exceeds this."
)

api_error_threshold = st.sidebar.number_input(
    "Alert: API Error Rate (%)",
    min_value=1,
    max_value=50,
    value=5,
    step=1,
    help="Trigger incident if API error rate exceeds this."
)

run_simulation = st.sidebar.button("🚀 Run Monitoring Simulation")

# ---------------------------
# MAIN LOGIC
# ---------------------------
if run_simulation:
    # Generate synthetic Swiggy-like data
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)
    df = generate_order_data(start_time, end_time)

    # Detect anomalies
    anomalies = detect_anomalies(
        df,
        order_drop_pct=order_drop_threshold,
        delivery_time_threshold=delivery_delay_threshold,
        payment_fail_threshold=payment_failure_threshold,
        api_error_threshold=api_error_threshold
    )

    # Log anomalies as incidents
    if not anomalies.empty:
        incident_log = log_incidents(anomalies)
    else:
        incident_log = load_incident_log()

    # ---------------------------
    # DASHBOARD LAYOUT
    # ---------------------------
    st.subheader("📊 Live KPI Overview")

    latest_row = df.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Current Orders / min",
        f"{latest_row['orders_per_min']:.0f}"
    )
    col2.metric(
        "Avg Delivery Time (min)",
        f"{latest_row['avg_delivery_time']:.1f}"
    )
    col3.metric(
        "Payment Failure Rate (%)",
        f"{latest_row['payment_failure_rate']:.2f}"
    )
    col4.metric(
        "API Error Rate (%)",
        f"{latest_row['api_error_rate']:.2f}"
    )

    # Time-series charts
    st.markdown("### 📈 KPI Trends over Time")

    kpi_tab1, kpi_tab2, kpi_tab3, kpi_tab4 = st.tabs(
        ["Orders", "Delivery Time", "Payment Failures", "API Errors"]
    )

    with kpi_tab1:
        st.line_chart(df.set_index("timestamp")["orders_per_min"])
    with kpi_tab2:
        st.line_chart(df.set_index("timestamp")["avg_delivery_time"])
    with kpi_tab3:
        st.line_chart(df.set_index("timestamp")["payment_failure_rate"])
    with kpi_tab4:
        st.line_chart(df.set_index("timestamp")["api_error_rate"])

    # ---------------------------
    # ANOMALIES & INCIDENTS
    # ---------------------------
    st.markdown("### 🚨 Detected Anomalies (Current Run)")

    if anomalies.empty:
        st.success("No anomalies detected in the current simulation window 🎉")
    else:
        st.error(f"{len(anomalies)} anomaly/anomalies detected in the current run.")
        st.dataframe(anomalies, use_container_width=True)

    st.markdown("### 🧾 Incident Log (Historical)")

    if incident_log is None or incident_log.empty:
        st.info("No incidents logged yet. Run the simulation to generate incidents.")
    else:
        st.caption(f"Incident log file: `{INCIDENT_LOG_FILE}`")
        st.dataframe(incident_log.tail(50), use_container_width=True)

else:
    st.info(
        "Click **'Run Monitoring Simulation'** in the left sidebar to generate "
        "simulated Swiggy-like order data and detect anomalies."
    )

# ---------------------------
# INCIDENT RESPONSE PLAN (IRP)
# ---------------------------
st.markdown("---")
st.header("📒 Incident Response Plan (IRP) – Swiggy-like Order Monitoring")

with st.expander("🔔 Alerts – When is an Incident Triggered?"):
    st.markdown(
        """
- **Order Volume Drop**  
  - Condition: Orders per minute drop more than configured % vs rolling baseline.  
  - Example: >40% drop compared to last 30 minutes average.

- **Delivery Delays**  
  - Condition: Average delivery time exceeds threshold for continuous window.  
  - Example: Avg delivery time > 50 minutes for 10+ minutes.

- **Payment Failures**  
  - Condition: Payment failure rate spikes beyond threshold.  
  - Example: Failure rate > 10%.

- **API Errors**  
  - Condition: 5xx error rate or timeout rate exceeds threshold.  
  - Example: API error rate > 5%.
        """
    )

with st.expander("🛠 Response – First Line Debugging Checklist"):
    st.markdown(
        """
1. **Check Monitoring Dashboard**
   - Confirm which KPI triggered the alert.
   - Validate if the spike/drop is across all cities or isolated.

2. **Inspect Logs**
   - Filter logs by time window of the incident.
   - Look for timeouts, 5xx responses, DB slow queries, or third-party failures.

3. **Validate Data Pipelines**
   - Check if order ingestion / event stream is delayed or stuck.
   - Verify no data corruption in critical tables (orders, payments, delivery status).

4. **Run Quick Health Checks**
   - API health endpoints.
   - Database connectivity checks.
   - Cache layer (Redis) / message queue (Kafka) lag.
        """
    )

with st.expander("🕵️ Tools to Debug – Observability & RCA"):
    st.markdown(
        """
- **Dashboards (Grafana-like / This UI)**  
  - Time-series KPIs for orders, delivery time, errors, and failures.

- **Logs**  
  - Application logs for errors, stack traces, and latency.
  - Payment gateway logs for specific transaction failures.

- **Data Tools**  
  - SQL queries to validate data completeness & integrity.
  - Notebook (Jupyter/Databricks) for deeper RCA on anomalies.

- **Metrics Store / Feature Store (Optional)**  
  - Verify that model features / metrics are fresh and not stale.
        """
    )

with st.expander("⬆️ Escalation Triggers – When to Escalate to DS/Engineering"):
    st.markdown(
        """
Escalate immediately when:

- **Data Quality Issues**
  - Null / corrupted values in key columns (order_id, payment_status, city).
  - Sudden schema changes from upstream systems.

- **Unexplained KPI Deviations**
  - Anomalies that cannot be explained by traffic surge, festival, rain, or outage.
  - Model output drift – e.g., ETA predictions suddenly very inaccurate.

- **Sustained Impact**
  - Alert remains active beyond agreed SLA (e.g., >15–30 minutes).
  - Revenue-impacting incidents (payment failures, mass order cancellations).

**On Escalation, provide:**
- Timestamp window of incident
- Affected KPIs and cities
- Screenshots/links to dashboards
- Initial RCA summary
- Steps already tried (retries, cache clear, service restart, etc.)
        """
    )

st.markdown(
    """
---
✅ *This demo app simulates the kind of incident monitoring, anomaly detection, and IRP discipline 
expected from an Associate Data Scientist / ML Ops / Monitoring Engineer in a Swiggy-like environment.*
"""
)
