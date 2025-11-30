import threading
import time

def simulate_db_operations(thread_id):
    print(f" Thread {thread_id} – Simulating insert/update/query...")
    time.sleep(1)
    print(f" Thread {thread_id} – Completed operation.")

def main():
    print(" Starting concurrent DB simulation...")

    threads = []
    for i in range(5):
        t = threading.Thread(target=simulate_db_operations, args=(i+1,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(" All concurrent operations completed.")

if __name__ == "__main__":
    main()
