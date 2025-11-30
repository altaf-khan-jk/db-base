import os
import requests
import pandas as pd
from pymongo import MongoClient
import mysql.connector


def fetch_climate_data():
    url = "https://api.worldbank.org/v2/country/CAN/indicator/EN.CLC.MDAT.ZS?format=json"
    print(f"Fetching data from API: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"API fetch failed with status code {response.status_code}")

    data = response.json()

    try:
        records = data[1]  # API returns [metadata, data]
        df = pd.DataFrame(records)
        print("\n Sample Climate Data (First 5 rows):")
        print(df.head())
        return df
    except Exception as e:
        print(" Error processing API response:", e)
        raise


def insert_into_mongodb(df):
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable not set.")

    print("\n Connecting to MongoDB...")
    client = MongoClient(mongo_uri)
    db = client["climate_db"]  #  DB name
    collection = db["weather_data"]  # collection name

    print(" Inserting data into MongoDB...")
    data_to_insert = df.to_dict("records")
    collection.insert_many(data_to_insert)
    print(f"✔ Inserted {len(data_to_insert)} records into MongoDB.")


def insert_into_mysql(df):
    mysql_password = os.getenv("MYSQL_ROOT_PASSWORD")
    if not mysql_password:
        raise ValueError("MYSQL_ROOT_PASSWORD environment variable not set.")

    print("\n Connecting to MySQL...")
    conn = mysql.connector.connect(
        host="mysql",  # GitHub Actions MySQL service name
        user="root",
        password=mysql_password,
        database="climate_db"
    )

    cursor = conn.cursor()

    print(" Inserting data into MySQL...")
    insert_query = """
        INSERT INTO weather_data (country, date, value)
        VALUES (%s, %s, %s)
    """

    # Insert only necessary fields
    inserted_count = 0
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row.get("countryiso3code", None),
            row.get("date", None),
            row.get("value", None)
        ))
        inserted_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f" Inserted {inserted_count} records into MySQL.")


def main():
    print("\n Starting ETL Process: Fetch → MongoDB → MySQL")
    
    df = fetch_climate_data()

    print("\n Step 1: Insert into MongoDB")
    insert_into_mongodb(df)

    print("\n Step 2: Insert into MySQL")
    insert_into_mysql(df)

    print("\n ETL Completed – Data loaded in both MongoDB and MySQL!")


if __name__ == "__main__":
    main()
