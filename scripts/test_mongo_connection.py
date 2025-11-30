import os
from pymongo import MongoClient

def test_connection():
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable is not set.")
    
    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        print("MongoDB connection successful!")
    except Exception as e:
        print("MongoDB connection failed:", e)
        raise

if __name__ == "__main__":
    test_connection()
