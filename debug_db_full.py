
import pymongo
import os
import json
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def check_db():
    with open("db_debug_full.txt", "w") as f:
        f.write(f"TIMESTAMP: {datetime.now()}\n\n")
        
        f.write("--- LATEST JOBS ---\n")
        for job in db.ingestion_jobs.find().sort("triggeredAt", -1).limit(5):
            # Convert ObjectId and datetime for json dump
            job_copy = job.copy()
            job_copy["_id"] = str(job_copy["_id"])
            if "triggeredAt" in job_copy and job_copy["triggeredAt"]:
                job_copy["triggeredAt"] = job_copy["triggeredAt"].isoformat()
            if "completedAt" in job_copy and job_copy["completedAt"]:
                job_copy["completedAt"] = job_copy["completedAt"].isoformat()
            
            f.write(json.dumps(job_copy, indent=2) + "\n")
        
        f.write("\n--- LATEST RECORDS (PatientData) ---\n")
        for rec in db.ingestion_records.find({"templateKey": "PatientData"}).sort("_id", -1).limit(5):
            rec_copy = rec.copy()
            rec_copy["_id"] = str(rec_copy["_id"])
            f.write(json.dumps(rec_copy, indent=2) + "\n")

if __name__ == "__main__":
    check_db()
    print("Full DB state written to db_debug_full.txt")
