
import sys
from db_config import get_db

db = get_db()
if db is None:
    sys.exit(1)

target_job_prefix = "71fa522d"

def check_snapshot():
    print(f"--- CHECKING SNAPSHOT FOR {target_job_prefix}... ---")
    
    # Find full Job ID
    job = db.ingestion_jobs.find_one({"jobId": {"$regex": f"^{target_job_prefix}"}})
    if not job:
        print("Job NOT FOUND.")
        return

    job_id = job["jobId"]
    print(f"Full Job ID: {job_id}")
    print(f"Status: {job.get('status')}")
    print(f"Upload ID: {job.get('uploadId')}")

    # Check Records
    records = list(db.ingestion_records.find({"jobId": job_id}))
    print(f"Total Records: {len(records)}")
    
    if not records:
        print("No ingestion records found.")
        return

    # Check first record for snapshot
    first_rec = records[0]
    print(f"Record 0 Status: {first_rec.get('status')}")
    
    if "processedData" in first_rec:
        print("SUCCESS: 'processedData' snapshot EXISTs.")
        print(f"Keys in processedData: {list(first_rec['processedData'].keys())}")
    else:
        print("FAILURE: 'processedData' snapshot MISSING.")
        print(f"Keys in record: {list(first_rec.keys())}")

if __name__ == "__main__":
    check_snapshot()
