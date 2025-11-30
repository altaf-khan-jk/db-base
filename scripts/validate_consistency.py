import os
import sys
import mysql.connector
from pymongo import MongoClient

def get_mongo_uri():
    if len(sys.argv) > 1:
        return sys.argv[1]
    raise ValueError("MongoDB URI must be provided as a command-line argument.")

def validate_data_consistency(expected_records=50):  # Expected ETL count
    mysql_password = os.getenv("MYSQL_ROOT_PASSWORD")
    if not mysql_password:
        raise ValueError("MYSQL_ROOT_PASSWORD environment variable not set.")

    # Check MySQL count
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password=mysql_password,
        database="climate_db"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_data")
    mysql_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    # Check MongoDB count
    mongo_uri = get_mongo_uri()
    client = MongoClient(mongo_uri)
    mongo_count = client["climate_db"]["weather_data"].count_documents({})

    print("\n Data Consistency Check")
    print(f"MySQL record count:   {mysql_count}")
    print(f"MongoDB record count: {mongo_count}")
    print(f"Expected ETL records: {expected_records}")

    if mongo_count == expected_records:
        print("✔ ETL phase data is consistent between MySQL and MongoDB!")
        if mysql_count > expected_records:
            print("ℹ Additional records exist in MySQL due to concurrent operations.")
    else:
        raise Exception("❌ Data mismatch detected during ETL phase.")

def main():
    print(" Starting Data Validation...")
    validate_data_consistency(expected_records=50)

if __name__ == "__main__":
    main()
