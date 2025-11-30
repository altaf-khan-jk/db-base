import os
import sys
import requests
import pandas as pd
from pymongo import MongoClient
import mysql.connector

# Get Mongo URI (supports mock mode)
def get_mongo_uri():
    if len(sys.argv) > 1:
        return sys.argv[1]
    raise ValueError("MongoDB URI must be provided as a CLI argument or 'mock' for CI testing")

def fetch_climate_data():
    url = "https://api.worldbank.org/v2/country/CAN/indicator/EN.CLC.MDAT.ZS?format=json"
    print(f"Fetching data from API: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API fetch failed: {response.status_code}")
    data = response.json()
    try:
        df = pd.DataFrame(data[1])  # Extract data
        print("\n Sample Climate Data:")
        print(df.head())
        return df
    except Exception as e:
        print("Error processing API response:", e)
        raise

def insert_into_mongodb(df, mongo_uri):
    if mongo_uri == "mock":
        print(" MongoDB load skipped (CI mock mode)")
        return
    print(f"\n Connecting to MongoDB ({mongo_uri})...")
    client = MongoClient(mongo_uri)
    db = client["climate_db"]
    collection = db["weather_data"]
    data_to_insert = df.to_dict("records")
    collection.insert_many(data_to_insert)
    print(f"✔ Inserted {len(data_to_insert)} records into MongoDB.")

def insert_into_mysql(df):
    password = os.getenv("MYSQL_ROOT_PASSWORD")
    if not password:
        raise ValueError("MYSQL_ROOT_PASSWORD not set.")
    print("\n Connecting to MySQL...")
    conn = mysql.connector.connect(host="127.0.0.1", user="root", password=password, database="climate_db")
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO weather_data (country, date, value)
        VALUES (%s, %s, %s)
    """
    inserted_count = 0
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row.get("countryiso3code") or None,
            row.get("date") or None,
            row.get("value") or None
        ))
        inserted_count += 1
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✔ Inserted {inserted_count} records into MySQL.")

def main():
    print("\n Starting ETL Process: Fetch → MongoDB → MySQL")
    mongo_uri = get_mongo_uri()
    df = fetch_climate_data()
    print("\n Step 1: Insert into MongoDB")
    insert_into_mongodb(df, mongo_uri)
    print("\n Step 2: Insert into MySQL")
    insert_into_mysql(df)
    print("\n ETL Completed Successfully")

if __name__ == "__main__":
    main()
