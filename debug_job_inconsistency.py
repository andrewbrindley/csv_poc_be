
import os
import pymongo
import json
from dotenv import load_dotenv
from datetime import datetime

# Load .env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "csv")

client = pymongo.MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

print(f"Checking Job ID prefix: 13188313")

# Find the job
job = db.ingestion_jobs.find_one({"jobId": {"$regex": "^13188313"}})

if not job:
    print("Job not found!")
    # List a few recent jobs to see if ID matches
    print("\nRecent Jobs:")
    for j in db.ingestion_jobs.find().sort("triggeredAt", -1).limit(5):
        print(f"  ID: {j.get('jobId')}, Status: {j.get('status')}, Metrics: {j.get('metrics')}")
else:
    job_id = job["jobId"]
    print(f"\nFOUND JOB: {job_id}")
    print(f"Status: {job['status']}")
    print(f"Metrics: {job.get('metrics')}")
    print(f"TriggeredAt: {job.get('triggeredAt')}")
    
    # Check Records
    print(f"\nChecking Records for {job_id}:")
    rec_counts = db.ingestion_records.aggregate([
        {"$match": {"jobId": job_id}},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
    ])
    for c in rec_counts:
        print(f"  Status '{c['_id']}': {c['count']}")
        
    resolved_recs = list(db.ingestion_records.find({"jobId": job_id, "status": "resolved"}).limit(5))
    print(f"\nSample Resolved Records ({len(resolved_recs)} total found in find):")
    for r in resolved_recs:
         print(f"  RowIndex: {r.get('rowIndex')}, Template: {r.get('templateKey')}, Data: {r.get('data')}")

    # Check Tenant Data Linked to this Job
    print(f"\nChecking Tenant Data linked to {job_id}:")
    td_count = db.tenant_data.count_documents({"jobId": job_id})
    print(f"  Count: {td_count}")
    for d in db.tenant_data.find({"jobId": job_id}).limit(3):
        print(f"  Template: {d.get('templateKey')}, Op: {d.get('__operation')}, Data: {d.get('data')}")

    # Check for "patientId: 7001" specifically
    print("\nChecking for patientId 7001/7002 across all tenant_data:")
    for d in db.tenant_data.find({"data.patientId": {"$in": ["7001", "7002", 7001, 7002]}}):
        print(f"  Found {d.get('templateKey')} - JobId: {d.get('jobId')}, Op: {d.get('__operation')}, Time: {d.get('timestamp')}")

