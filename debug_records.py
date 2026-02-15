
import pymongo
import os
import json
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def check_db():
    with open("db_records_debug.txt", "w") as f:
        f.write("--- LATEST INGESTION RECORDS (NO FILTER) ---\n")
        for rec in db.ingestion_records.find().sort("_id", -1).limit(10):
            rec_copy = rec.copy()
            rec_copy["_id"] = str(rec_copy["_id"])
            for key, val in rec_copy.items():
                if isinstance(val, datetime):
                    rec_copy[key] = val.isoformat()
            f.write(json.dumps(rec_copy, indent=2) + "\n")

if __name__ == "__main__":
    check_db()
    print("Record state written to db_records_debug.txt")
