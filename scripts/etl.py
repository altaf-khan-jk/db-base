import os
import sys
import requests
import pandas as pd
from pymongo import MongoClient
import mysql.connector


def get_mongo_uri():
    """
    Get MongoDB URI from CLI argument.
    In CI we pass 'mock' to skip Mongo work.
    """
    if len(sys.argv) > 1:
        return sys.argv[1]
    raise ValueError("MongoDB URI must be provided as a CLI argument, or 'mock' for CI.")


def fetch_climate_data():
    url = "https://api.worldbank.org/v2/country/CAN/indicator/EN.CLC.MDAT.ZS?format=json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    print(f"Fetching data from API: {url}")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"API fetch failed with status code {response.status_code}")

    data = response.json()

    try:
        df = pd.DataFrame(data[1])  # API returns [metadata, data]
        print("\n Sample Climate Data:")
        print(df.head())
        return df
    except Exception as e:
        print(" Error processing API response:", e)
        raise


def insert_into_mongodb(df, mongo_uri: str):
    """
    Insert data into MongoDB.
    In CI ('mock' mode) this is skipped.
    """
    if mongo_uri == "mock":
        print(" MongoDB load skipped (CI mock mode)")
        return

    print(f"\n Connecting to MongoDB ({mongo_uri})...")
    client = MongoClient(mongo_uri)
    db = client["climate_db"]
    collection = db["weather_data"]

    print(" Inserting data into MongoDB...")
    data_to_insert = df.to_dict("records")
    collection.insert_many(data_to_insert)
    print(f"✔ Inserted {len(data_to_insert)} records into MongoDB.")


def insert_into_mysql(df: pd.DataFrame):
    """
    Insert API data into MySQL.
    Any NaN / missing values are converted to NULL before insert.
    """
    mysql_password = os.getenv("MYSQL_ROOT_PASSWORD")
    if not mysql_password:
        raise ValueError("MYSQL_ROOT_PASSWORD environment variable not set.")

    print("\n Connecting to MySQL...")
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password=mysql_password,
        database="climate_db",
    )
    cursor = conn.cursor()

    print(" Inserting data into MySQL...")

    insert_query = """
        INSERT INTO weather_data (country, date, value)
        VALUES (%s, %s, %s)
    """

    inserted_count = 0

    for _, row in df.iterrows():
        country = row.get("countryiso3code", None)
        date = row.get("date", None)
        value = row.get("value", None)

        # Convert pandas / numpy NaN to None so MySQL stores NULL
        if pd.isna(country):
            country = None
        if pd.isna(date):
            date = None
        if pd.isna(value):
            value = None

        cursor.execute(insert_query, (country, date, value))
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

    print("\n  ETL Completed – Data loaded in both (or MySQL only in mock mode)!")


if __name__ == "__main__":
    main()
