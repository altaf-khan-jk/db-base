import os
import warnings
import pandas as pd
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from pymongo import MongoClient
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score
import psycopg2
warnings.filterwarnings("ignore")


# --------------------------------------------
# DB CONNECTIONS (EDIT IF NEEDED)
# --------------------------------------------
MONGO_URI = "mongodb://localhost:27017"
POSTGRES_CONFIG = {
    "dbname": "transportdb",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432
}
TEMPERATURE_SPIKE_THRESHOLD = 70
FARE_SPIKE_THRESHOLD = 150
METRIC_EXPORT_INTERVAL_MS = int(os.getenv("ANOMALY_METRIC_EXPORT_INTERVAL_MS", "10000"))
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
)


# --------------------------------------------
# SIGNOZ METRICS SETUP
# --------------------------------------------
def setup_metrics():
    """Configure OTLP metric export so SigNoz can ingest anomaly metrics."""

    resource = Resource.create({
        ResourceAttributes.SERVICE_NAME: "anomaly-detector",
        ResourceAttributes.SERVICE_NAMESPACE: "database-monitoring",
    })

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=OTEL_EXPORTER_OTLP_ENDPOINT, insecure=True),
        export_interval_millis=METRIC_EXPORT_INTERVAL_MS,
    )

    provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(provider)
    meter = metrics.get_meter_provider().get_meter("db-base.anomaly", "0.1.0")

    detection_runs = meter.create_counter(
        "anomaly_detection_runs",
        description="Number of anomaly detection executions",
    )
    anomaly_histogram = meter.create_histogram(
        "anomaly_records_flagged",
        unit="1",
        description="Count of records flagged as anomalies per run",
    )
    precision_histogram = meter.create_histogram(
        "anomaly_detection_precision",
        unit="1",
        description="Observed precision for anomaly detection runs",
    )
    recall_histogram = meter.create_histogram(
        "anomaly_detection_recall",
        unit="1",
        description="Observed recall for anomaly detection runs",
    )

    return {
        "meter": meter,
        "detection_runs": detection_runs,
        "anomaly_histogram": anomaly_histogram,
        "precision_histogram": precision_histogram,
        "recall_histogram": recall_histogram,
    }


METRICS = setup_metrics()

# --------------------------------------------
# LOAD SAMPLE DATA
# --------------------------------------------
def load_data():
    df = pd.DataFrame({
        "temperature": [20, 21, 22, 23, 80, 21, 22],   # 80 is anomalous spike
        "fare": [10, 11, 9, 10, 500, 8, 12],           # 500 is outlier fare
        "humidity": [40, 42, None, 41, 43, 300, 44]    # None + 300 are anomalies
    })
    return df


# --------------------------------------------
# DETECT ANOMALIES
# --------------------------------------------
def detect_anomalies(df):
    df["missing_value"] = df.isna().any(axis=1)
    df["temperature_spike"] = df["temperature"] > TEMPERATURE_SPIKE_THRESHOLD
    df["fare_outlier"] = df["fare"] > FARE_SPIKE_THRESHOLD
    model = IsolationForest(contamination=0.15, random_state=42)
    df_clean = df.fillna(df.median(numeric_only=True)).copy()
    model = IsolationForest(contamination=0.15, random_state=42)
    df["ml_anomaly"] = model.fit_predict(df_clean)
    df["ml_anomaly"] = df["ml_anomaly"].apply(lambda x: 1 if x == -1 else 0)

    df["is_anomaly"] = (
        df[["missing_value", "temperature_spike", "fare_outlier", "ml_anomaly"]]
        .any(axis=1)
        .astype(int)
    )
    return df



# --------------------------------------------
# STORE ANOMALIES IN MONGODB
# --------------------------------------------
def store_in_mongo(anomalies):
    if anomalies.empty:
        print("ℹ️  No anomalies to store in MongoDB.")
        return
    client = MongoClient(MONGO_URI)
    db = client["transportdb"]
    anomalies_col = db["Anomalies"]
    anomalies_col.insert_many(anomalies.to_dict(orient="records"))
    print("✔ Anomalies stored in MongoDB collection: Anomalies")


# --------------------------------------------
# STORE ANOMALIES IN POSTGRESQL
# --------------------------------------------
def store_in_postgres(anomalies):
    if anomalies.empty:
        print("ℹ️  No anomalies to store in PostgreSQL.")
        return
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            id SERIAL PRIMARY KEY,
            temperature FLOAT,
            fare FLOAT,
            humidity FLOAT,
            is_anomaly INT
        );
    """)

    rows = [
        (
            float(row["temperature"]) if row["temperature"] is not None else None,
            float(row["fare"]) if row["fare"] is not None else None,
            float(row["humidity"]) if row["humidity"] is not None else None,
            int(row["is_anomaly"]),
        )
        for _, row in anomalies.iterrows()
    ]
    cur.executemany(
        "INSERT INTO anomalies (temperature, fare, humidity, is_anomaly) VALUES (%s, %s, %s, %s)",
        rows,
    )

    conn.commit()
    cur.close()
    conn.close()
    print("✔ Anomalies stored in PostgreSQL table: anomalies")


# --------------------------------------------
# PERFORMANCE METRICS (Precision & Recall)
# --------------------------------------------
def calculate_metrics(df):
    df["true_label"] = (
        (
            (df["temperature"] > TEMPERATURE_SPIKE_THRESHOLD)
            | (df["fare"] > FARE_SPIKE_THRESHOLD)
            | (df["humidity"].isna())
        )
        .astype(int)
    )
    precision = precision_score(df["true_label"], df["is_anomaly"])
    recall = recall_score(df["true_label"], df["is_anomaly"])
    return precision, recall

def record_metrics(anomalies, precision, recall):
    if not METRICS:
        return

    try:
        METRICS["detection_runs"].add(1)
        METRICS["anomaly_histogram"].record(len(anomalies))
        METRICS["precision_histogram"].record(float(precision))
        METRICS["recall_histogram"].record(float(recall))
    except Exception as exc:  # noqa: BLE001
        # Metric export should not break the data pipeline
        print(f"⚠️  Unable to export metrics to SigNoz: {exc}")


# --------------------------------------------
# MAIN PIPELINE
# --------------------------------------------
def run_anomaly_detection():
    print("▶ Loading data...")
    df = load_data()

    print("▶ Detecting anomalies...")
    df = detect_anomalies(df)

    anomalies = df[df["is_anomaly"] == 1]

    print("▶ Storing anomalies in MongoDB and PostgreSQL...")
    store_in_mongo(anomalies)
    store_in_postgres(anomalies)

    print("▶ Calculating detection metrics...")
    precision, recall = calculate_metrics(df)
    record_metrics(anomalies, precision, recall)


    print("\n===== ANOMALY DETECTION REPORT =====")
    print(df)
    print(f"\nPrecision: {precision}")
    print(f"Recall: {recall}")
    print("====================================\n")


if __name__ == "__main__":
    run_anomaly_detection()
