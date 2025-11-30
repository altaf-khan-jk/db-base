import threading
import os
import mysql.connector
import time


def simulate_db_operations(thread_id):
    mysql_password = os.getenv("MYSQL_ROOT_PASSWORD")
    conn = mysql.connector.connect(
        host="mysql",
        user="root",
        password=mysql_password,
        database="climate_db"
    )
    cursor = conn.cursor()

    print(f" Thread {thread_id} starting DB operations...")

    # Example insert
    insert_query = "INSERT INTO weather_data (country, date, value) VALUES (%s, %s, %s)"
    cursor.execute(insert_query, (f"T{thread_id}", "2025", thread_id * 10))
    conn.commit()
    print(f" Thread {thread_id} INSERT completed.")

    time.sleep(1)

    # Example query
    cursor.execute("SELECT COUNT(*) FROM weather_data")
    count = cursor.fetchone()[0]
    print(f" Thread {thread_id} DB row count: {count}")

    time.sleep(1)

    # Example update
    cursor.execute("UPDATE weather_data SET value = value + 1 WHERE country = %s", (f"T{thread_id}",))
    conn.commit()
    print(f" Thread {thread_id} UPDATE completed.")

    cursor.close()
    conn.close()
    print(f" Thread {thread_id} completed all DB tasks.\n")


def main():
    print(" Starting REAL concurrent database operations...")

    threads = []
    for i in range(5):
        t = threading.Thread(target=simulate_db_operations, args=(i + 1,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(" All concurrent operations completed successfully!")


if __name__ == "__main__":
    main()
