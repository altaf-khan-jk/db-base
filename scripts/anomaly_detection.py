#!/usr/bin/env python3
"""
Anomaly detection module for Task 3.

Usage:
  python scripts/anomaly_detection.py <mongo_uri> [--label-file labels.csv]

Notes:
- Expects a MySQL database `climate_db` and table `weather_data(id, country, date, value)`.
- Writes flagged rows to MySQL table `anomalies` and MongoDB collection 'anomalies'.
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from pymongo import MongoClient
import mysql.connector
from datetime import datetime

def get_mysql_conn():
    pw = os.getenv("MYSQL_ROOT_PASSWORD", "root")
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        password=pw,
        database="climate_db"
    )

def load_data_from_mysql(limit=None):
    conn = get_mysql_conn()
    query = "SELECT id, country, date, value FROM weather_data"
    if limit:
        query += f" LIMIT {limit}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def detect_anomalies_isolationforest(df, contamination=0.01):
    """
    Returns DataFrame with columns: id,country,date,value,score,is_anomaly
    score: the negative outlier factor from IsolationForest (the lower, the more anomalous)
    """
    result_rows = []
    # if few rows overall, do a single model; else model per country
    groups = [("ALL", df)] if df['country'].nunique() <= 1 else df.groupby("country")
    for group_name, sub in (groups if isinstance(groups, list) else groups):
        sub = sub.copy()
        if len(sub) < 10:
            # fallback to z-score if group too small
            sub['score'] = (sub['value'] - sub['value'].mean()) / (sub['value'].std(ddof=0) + 1e-9)
            sub['is_anomaly'] = sub['score'].abs() > 3.0
            sub['method'] = 'zscore'
        else:
            model = IsolationForest(contamination=contamination, random_state=42)
            X = sub[['value']].values
            model.fit(X)
            # score_samples -> higher = normal, lower = anomalous (we'll use -score to have "lower is worse")
            raw_scores = model.score_samples(X)  # higher is better
            sub['score'] = -raw_scores
            sub['is_anomaly'] = model.predict(X) == -1
            sub['method'] = 'isolation_forest'
        result_rows.append(sub[['id','country','date','value','score','is_anomaly','method']])
    res = pd.concat(result_rows, ignore_index=True)
    return res

def write_anomalies_mysql(df_anom):
    if df_anom.shape[0] == 0:
        print("No anomalies to write to MySQL.")
        return
    conn = get_mysql_conn()
    cursor = conn.cursor()
    insert_sql = """
        INSERT INTO anomalies (source_id, country, date, value, score, method)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    rows = []
    for _, r in df_anom.iterrows():
        rows.append((int(r['id']), r['country'], r['date'], float(r['value']), float(r['score']), r['method']))
    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Wrote {len(rows)} anomalies to MySQL anomalies table.")

def write_anomalies_mongo(df_anom, mongo_uri):
    if df_anom.shape[0] == 0:
        print("No anomalies to write to MongoDB.")
        return
    if mongo_uri.lower() == 'mock':
        print("MongoDB is mocked — skipping Mongo write.")
        return
    client = MongoClient(mongo_uri)
    # default database name: climate_db
    db = client.get_database('climate_db') if 'climate_db' in client.list_database_names() else client.climate_db
    coll = db.anomalies
    docs = []
    for _, r in df_anom.iterrows():
        docs.append({
            "source_id": int(r['id']),
            "country": r['country'],
            "date": r['date'],
            "value": float(r['value']),
            "score": float(r['score']),
            "method": r['method'],
            "detected_at": datetime.utcnow()
        })
    coll.insert_many(docs)
    client.close()
    print(f"Wrote {len(docs)} anomalies to MongoDB collection 'anomalies'.")

def evaluate_if_labels(df_flagged, label_file):
    """
    If label_file provided, compute precision/recall.
    label_file must be CSV with column 'id' or 'source_id' marking true anomalies.
    """
    if not label_file:
        print("No label file provided — cannot compute precision/recall.")
        return
    labels = pd.read_csv(label_file)
    if 'source_id' in labels.columns:
        true_ids = set(labels['source_id'].astype(int).tolist())
    elif 'id' in labels.columns:
        true_ids = set(labels['id'].astype(int).tolist())
    else:
        print("Label file must contain 'id' or 'source_id' column.")
        return
    detected_ids = set(df_flagged['id'].astype(int).tolist())
    tp = len(detected_ids & true_ids)
    fp = len(detected_ids - true_ids)
    fn = len(true_ids - detected_ids)
    precision = tp / (tp + fp) if tp + fp > 0 else 0.0
    recall = tp / (tp + fn) if tp + fn > 0 else 0.0
    print("Evaluation with provided labels:")
    print(f"  True Positives: {tp}, False Positives: {fp}, False Negatives: {fn}")
    print(f"  Precision: {precision:.3f}, Recall: {recall:.3f}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mongo_uri", help="MongoDB URI or 'mock'")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows from weather_data for quick testing")
    parser.add_argument("--label-file", default=None, help="Optional CSV file with true anomaly ids")
    parser.add_argument("--contamination", type=float, default=0.01, help="Expected fraction of anomalies")
    args = parser.parse_args()

    print("Loading data from MySQL...")
    df = load_data_from_mysql(limit=args.limit)
    if df.empty:
        print("No data found in weather_data. Run ETL first.")
        sys.exit(1)
    print(f"Loaded {len(df)} rows.")

    print("Running anomaly detection...")
    df_res = detect_anomalies_isolationforest(df, contamination=args.contamination)
    flagged = df_res[df_res['is_anomaly'] == True].copy()
    print(f"Detected {len(flagged)} anomalies (method breakdown):")
    print(flagged['method'].value_counts().to_dict() if len(flagged)>0 else {})

    # Write to storage
    write_anomalies_mysql(flagged)
    write_anomalies_mongo(flagged, args.mongo_uri)

    # Optional evaluation
    if args.label_file:
        evaluate_if_labels(flagged, args.label_file)

    print("Done.")

if __name__ == "__main__":
    main()
