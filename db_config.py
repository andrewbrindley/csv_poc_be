import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load .env from the same directory as this file
base_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "csv")

# Masked logging for verification
masked_uri = MONGO_URI.split("@")[-1] if "@" in MONGO_URI else "LOCAL/UNKNOWN"
print(f"STARTUP: Attempting connection to Database: {MONGO_DB_NAME} on Host: {masked_uri}")

# Global DB instance
_db = None

def get_db():
    """
    Connects to MongoDB and returns the database object.
    Uses a singleton pattern to avoid reconnecting on every request.
    """
    global _db
    if _db is not None:
        return _db
        
    try:
        print("DB: Establishing new connection...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Verify connection on startup/first access only
        client.server_info()
        _db = client[MONGO_DB_NAME]
        print("DB: Connection successful.")
        return _db
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return None

# Helper functions for collections
def get_imports_collection():
    db = get_db()
    if db is not None:
        return db["imports"]
    return None

def get_tenant_data_collection():
    db = get_db()
    if db is not None:
        return db["tenant_data"]
    return None

def get_raw_uploads_collection():
    db = get_db()
    if db is not None:
        # User explicitly wants 'raw_uploads' in the 'csv' database
        return db["raw_uploads"]
    return None

def get_ingestion_jobs_collection():
    db = get_db()
    if db is not None:
        # User explicitly wants 'ingestion_jobs' in the 'csv' database
        return db["ingestion_jobs"]
    return None

def get_ingestion_records_collection():
    db = get_db()
    if db is not None:
        return db["ingestion_records"]
    return None
