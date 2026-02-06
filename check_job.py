
import sys
from pymongo import MongoClient
from db_config import get_db

if db is None:
    sys.exit(1)

target_job_id = "f79db812" # from screenshot... wait, I need the full ID.
# Actually, the screenshot shows "f79db812" which is a prefix.
# I'll search for it.

print(f"Searching for job starting with {target_job_id}...")
job = db.ingestion_jobs.find_one({"jobId": {"$regex": f"^{target_job_id}"}})

if job:
    full_id = job["jobId"]
    print(f"Found Job: {full_id}")
    print(f"Triggered: {job.get('triggeredAt')}")
    
    count = db.tenant_data.count_documents({"jobId": full_id})
    print(f"Linked Data Count: {count}")
    
    if count == 0:
        print("CONFIRMED: This job has NO linked data.")
else:
    print("Job not found.")
