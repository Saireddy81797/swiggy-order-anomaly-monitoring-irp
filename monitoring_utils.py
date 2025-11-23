import pandas as pd
import numpy as np
from datetime import timedelta
import random

CITIES = ["Bangalore", "Hyderabad", "Chennai", "Mumbai", "Pune"]


def generate_order_data(start_time, end_time, freq="1min"):
    """
    Generate synthetic Swiggy-like order data between start_time and end_time.
    """
    timestamps = pd.date_range(start=start_time, end=end_time, freq=freq)
    n = len(timestamps)

    # base patterns
    base_orders = np.random.randint(80, 150, size=n).astype(float)
    base_delivery_time = np.random.normal(loc=35, scale=5, size=n)
    base_payment_failure = np.random.uniform(0.5, 3.0, size=n)  # %
    base_api_error_rate = np.random.uniform(0.1, 1.5, size=n)   # %

    # occasionally inject anomalies
    df = pd.DataFrame({
        "timestamp": timestamps,
        "orders_per_min": base_orders,
        "avg_delivery_time": base_delivery_time,
        "payment_failure_rate": base_payment_failure,
        "api_error_rate": base_api_error_rate,
    })

    # assign random city per row (simplified)
    df["city"] = np.random.choice(CITIES, size=n)

    # inject some spikes/drops
    for _ in range(3):
        idx = np.random.randint(0, n)
        # random type of anomaly
        anomaly_type = random.choice(["orders_drop", "delivery_spike", "payment_spike", "api_spike"])

        if anomaly_type == "orders_drop":
            df.loc[idx: idx + 3, "orders_per_min"] *= np.random.uniform(0.1, 0.4)
        elif anomaly_type == "delivery_spike":
            df.loc[idx: idx + 5, "avg_delivery_time"] *= np.random.uniform(1.5, 2.5)
        elif anomaly_type == "payment_spike":
            df.loc[idx: idx + 3, "payment_failure_rate"] *= np.random.uniform(3, 5)
        elif anomaly_type == "api_spike":
            df.loc[idx: idx + 3, "api_error_rate"] *= np.random.uniform(3, 6)

    # make sure no negative values
    df["orders_per_min"] = df["orders_per_min"].clip(lower=0)
    df["avg_delivery_time"] = df["avg_delivery_time"].clip(lower=0)
    df["payment_failure_rate"] = df["payment_failure_rate"].clip(lower=0)
    df["api_error_rate"] = df["api_error_rate"].clip(lower=0)

    return df


def detect_anomalies(
    df: pd.DataFrame,
    order_drop_pct: float,
    delivery_time_threshold: float,
    payment_fail_threshold: float,
    api_error_threshold: float
) -> pd.DataFrame:
    """
    Detect anomalies based on simple thresholds and rolling stats.
    Returns subset of df with additional 'alert_type' and 'severity'.
    """
    df = df.copy()
    df["rolling_orders"] = df["orders_per_min"].rolling(window=30, min_periods=10).mean()
    df["orders_drop_pct"] = (
        (df["rolling_orders"] - df["orders_per_min"]) / df["rolling_orders"].replace(0, np.nan)
    ) * 100

    conditions = []

    # Order volume drop
    cond_orders = df["orders_drop_pct"] > order_drop_pct
    if cond_orders.any():
        df.loc[cond_orders, "alert_type"] = df.get("alert_type", "") + "ORDER_VOLUME_DROP;"
        conditions.append(cond_orders)

    # Delivery time spike
    cond_delivery = df["avg_delivery_time"] > delivery_time_threshold
    if cond_delivery.any():
        df.loc[cond_delivery, "alert_type"] = df.get("alert_type", "") + "DELIVERY_DELAY;"
        conditions.append(cond_delivery)

    # Payment failure spike
    cond_payment = df["payment_failure_rate"] > payment_fail_threshold
    if cond_payment.any():
        df.loc[cond_payment, "alert_type"] = df.get("alert_type", "") + "PAYMENT_FAILURE_SPIKE;"
        conditions.append(cond_payment)

    # API error spike
    cond_api = df["api_error_rate"] > api_error_threshold
    if cond_api.any():
        df.loc[cond_api, "alert_type"] = df.get("alert_type", "") + "API_ERROR_SPIKE;"
        conditions.append(cond_api)

    if not conditions:
        return pd.DataFrame(columns=list(df.columns) + ["severity", "summary"])

    combined = conditions[0]
    for c in conditions[1:]:
        combined |= c

    anomalies = df[combined].copy()

    # Simple severity tagging
    def classify_severity(row):
        score = 0
        if "ORDER_VOLUME_DROP" in str(row.get("alert_type", "")):
            score += 2
        if "DELIVERY_DELAY" in str(row.get("alert_type", "")):
            score += 2
        if "PAYMENT_FAILURE_SPIKE" in str(row.get("alert_type", "")):
            score += 3
        if "API_ERROR_SPIKE" in str(row.get("alert_type", "")):
            score += 3

        if score >= 6:
            return "CRITICAL"
        elif score >= 3:
            return "HIGH"
        else:
            return "MEDIUM"

    anomalies["severity"] = anomalies.apply(classify_severity, axis=1)

    # human-readable summary for incident report
    def summarize(row):
        parts = []
        if "ORDER_VOLUME_DROP" in str(row["alert_type"]):
            parts.append("Orders dropped vs baseline")
        if "DELIVERY_DELAY" in str(row["alert_type"]):
            parts.append("Delivery time spiked")
        if "PAYMENT_FAILURE_SPIKE" in str(row["alert_type"]):
            parts.append("Payment failures spiked")
        if "API_ERROR_SPIKE" in str(row["alert_type"]):
            parts.append("API errors spiked")
        return ", ".join(parts)

    anomalies["summary"] = anomalies.apply(summarize, axis=1)

    return anomalies[[
        "timestamp", "city", "orders_per_min", "avg_delivery_time",
        "payment_failure_rate", "api_error_rate", "orders_drop_pct",
        "alert_type", "severity", "summary"
    ]]
