import os
import pymongo
from dotenv import load_dotenv

# Load .env
base_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "csv")

TENANT_ID = "acme-corp"

def reset_tenant_data():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        print(f"Connected to DB: {MONGO_DB_NAME}")
        print(f"Resetting data for tenant: {TENANT_ID}...")

        collections = [
            "tenant_data",
            "ingestion_records",
            "ingestion_jobs",
            "raw_uploads",
            "imports"
        ]

        for col_name in collections:
            coll = db[col_name]
            # Delete documents matching tenantId
            result = coll.delete_many({"tenantId": TENANT_ID})
            print(f"Deleted {result.deleted_count} documents from '{col_name}'")

        print("\nReset complete.")
        
    except Exception as e:
        print(f"Error resetting data: {e}")

if __name__ == "__main__":
    confirmation = input(f"Are you sure you want to delete ALL data for '{TENANT_ID}'? (yes/no): ")
    if confirmation.lower() == "yes":
        reset_tenant_data()
    else:
        print("Operation cancelled.")
