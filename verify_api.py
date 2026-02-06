
import requests
import time
import json
import sys

BASE_URL = "http://localhost:5000/api"
TENANT_ID = "acme-corp"
EXISTING_UPLOAD_ID = "1038173c-6509-4b11-8124-b8142e6c59a4" # From previous job, assumed valid

def run_verification():
    print(f"--- API DEEP DIVE VERIFICATION ---")
    
    # 1. Trigger Job
    print(f"1. Triggering new job for existing upload: {EXISTING_UPLOAD_ID}...")
    try:
        resp = requests.post(f"{BASE_URL}/import/trigger-job", json={
            "uploadId": EXISTING_UPLOAD_ID,
            "tenantId": TENANT_ID
        })
        if resp.status_code != 200:
            print(f"FAILED to trigger job: {resp.text}")
            return
        
        job_data = resp.json()
        job_id = job_data["jobId"]
        print(f"   SUCCESS. New Job ID: {job_id}")
    except Exception as e:
        print(f"CRITICAL ERROR calling API: {e}")
        return

    # 2. Poll for Completion
    print(f"2. Waiting for job {job_id} to complete...")
    status = "pending"
    attempts = 0
    while status not in ["completed", "error"] and attempts < 10:
        time.sleep(1)
        try:
            r = requests.get(f"{BASE_URL}/jobs/{job_id}")
            j = r.json().get("job", {})
            status = j.get("status")
            print(f"   Status: {status}")
        except:
            pass
        attempts += 1
        
    if status != "completed":
        print(f"FAILED: Job finished with status {status}")
        return

    # 3. Check Data
    print(f"3. Verifying data for Job {job_id}...")
    try:
        r = requests.get(f"{BASE_URL}/data", params={"tenantId": TENANT_ID, "jobId": job_id})
        data = r.json().get("data", [])
        count = len(data)
        print(f"   Records Found: {count}")
        
        if count > 0:
            print(f"   First Record ID: {data[0].get('id')}")
            print(f"VERIFICATION PASSED: System is processing and linking data correctly.")
        else:
            print(f"VERIFICATION FAILED: Job completed but returned 0 records.")
            
    except Exception as e:
        print(f"Error Checking Data: {e}")

if __name__ == "__main__":
    run_verification()
