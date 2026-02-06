
import os
import sys
from pymongo import MongoClient
from bson import ObjectId
import pprint

# Connect to DB
try:
    from db_config import get_db
    db = get_db()
except ImportError:
    # Fallback if run from wrong dir
    client = MongoClient("mongodb://localhost:27017/")
    db = client["csv"]

def scan_latest_job():
    print("--- 1. LATEST JOB ---")
    jobs_coll = db["ingestion_jobs"]
    job = jobs_coll.find_one({}, sort=[("triggeredAt", -1)])
    
    if not job:
        print("No jobs found.")
        return
    
    job_id = job.get("jobId")
    print(f"Job ID: {job_id}")
    print(f"Status: {job.get('status')}")
    print(f"Created: {job.get('triggeredAt')}")
    print(f"Plan: {job.get('jobPlan', {}).get('executionOrder')}")
    
    print("\n--- 2. INGESTION RECORDS ---")
    records_coll = db["ingestion_records"]
    count = records_coll.count_documents({"jobId": job_id})
    print(f"Total Records: {count}")
    
    pending = records_coll.count_documents({"jobId": job_id, "status": "pending"})
    resolved = records_coll.count_documents({"jobId": job_id, "status": "resolved"})
    error = records_coll.count_documents({"jobId": job_id, "status": "error"})
    print(f"Pending: {pending}, Resolved: {resolved}, Error: {error}")
    
    if error > 0:
        print("Sample Error:")
        err_doc = records_coll.find_one({"jobId": job_id, "status": "error"})
        pprint.pprint(err_doc)

    print("\n--- 3. TENANT DATA (Linked to Job) ---")
    data_coll = db["tenant_data"]
    linked_count = data_coll.count_documents({"jobId": job_id})
    print(f"Documents with jobId={job_id}: {linked_count}")
    
    if linked_count == 0:
        print("Checking if data exists but mostly unlinked...")
        # Check if any data exists for this tenant/template
        sample_rec = records_coll.find_one({"jobId": job_id})
        if sample_rec:
            t_key = sample_rec.get("templateKey")
            print(f"Template Key from record: {t_key}")
            total_tpl = data_coll.count_documents({"templateKey": t_key})
            print(f"Total docs for {t_key}: {total_tpl}")
            
            # Show a sample doc to see what its jobId is
            sample_doc = data_coll.find_one({"templateKey": t_key})
            if sample_doc:
                print("Sample Doc (Partial):")
                print(f"_id: {sample_doc.get('_id')}")
                print(f"jobId: {sample_doc.get('jobId')}")

if __name__ == "__main__":
    scan_latest_job()
