import os
import mysql.connector
import threading

def run_mysql_query():
    print("⚙ Running concurrent MySQL operation...")
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password=os.getenv("MYSQL_ROOT_PASSWORD"),
        database="climate_db"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_data")
    count = cursor.fetchone()[0]
    print(f"MySQL records during concurrency check: {count}")
    cursor.close()
    conn.close()

def main():
    print("\n🔁 Starting concurrent DB operations...")
    threads = []
    for _ in range(5):
        t = threading.Thread(target=run_mysql_query)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print("✔ Concurrency test completed")

if __name__ == "__main__":
    main()
