
import sys
from db_config import get_db

db = get_db()
if db is None:
    sys.exit(1)

target_job_id = "560168a0-e479-4db7-b138-b650077b2af4"

print(f"--- DIAGNOSTIC FOR {target_job_id} ---")

# 1. Check Job Tenant
job = db.ingestion_jobs.find_one({"jobId": target_job_id})
if job:
    print(f"JOB found.")
    print(f"Job TenantID: '{job.get('tenantId')}'")
else:
    print("JOB NOT FOUND.")

# 2. Check Data Tenant
data_count = db.tenant_data.count_documents({"jobId": target_job_id})
print(f"Data Records Linked: {data_count}")

if data_count > 0:
    sample = db.tenant_data.find_one({"jobId": target_job_id})
    print(f"Data TenantID: '{sample.get('tenantId')}'")
    
    # 3. Check mismatch
    if job and sample and job.get("tenantId") != sample.get("tenantId"):
        print("!!! MISMATCH DETECTED !!!")
else:
    print("No data records to check.")

# 4. Check Raw Upload Tenant
if job:
    upload_id = job.get("uploadId")
    upload = db.raw_uploads.find_one({"uploadId": upload_id})
    if upload:
        print(f"Upload TenantID: '{upload.get('tenantId')}'")
