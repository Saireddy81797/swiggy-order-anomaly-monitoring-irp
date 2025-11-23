import os
import pandas as pd
from datetime import datetime

INCIDENT_LOG_FILE = "incident_log.csv"


def load_incident_log():
    if not os.path.exists(INCIDENT_LOG_FILE):
        return None
    try:
        return pd.read_csv(INCIDENT_LOG_FILE, parse_dates=["timestamp"])
    except Exception:
        return None


def log_incidents(anomalies: pd.DataFrame) -> pd.DataFrame:
    """
    Append anomalies as incident rows into the incident_log.csv file.
    """
    if anomalies is None or anomalies.empty:
        return load_incident_log()

    incident_log = load_incident_log()
    new_incidents = anomalies.copy()

    # Add standard incident fields
    new_incidents["incident_id"] = [
        f"INC-{datetime.now().strftime('%Y%m%d%H%M%S')}-{i}"
        for i in range(len(new_incidents))
    ]
    new_incidents["status"] = "OPEN"
    new_incidents["created_at"] = datetime.now()

    # Example resolution note placeholder (can be updated later manually)
    new_incidents["resolution_notes"] = "Pending RCA and resolution."

    cols_order = [
        "incident_id",
        "timestamp",
        "city",
        "orders_per_min",
        "avg_delivery_time",
        "payment_failure_rate",
        "api_error_rate",
        "orders_drop_pct",
        "alert_type",
        "severity",
        "summary",
        "status",
        "created_at",
        "resolution_notes",
    ]

    new_incidents = new_incidents[cols_order]

    if incident_log is None or incident_log.empty:
        combined = new_incidents
    else:
        combined = pd.concat([incident_log, new_incidents], ignore_index=True)

    combined.to_csv(INCIDENT_LOG_FILE, index=False)
    return combined
