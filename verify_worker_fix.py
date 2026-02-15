
import requests
import time
import uuid

BASE_URL = "http://localhost:5000"
TENANT_ID = "test-tenant"

def test_patient_job():
    # 1. Prepare data with invalid patientId
    rows = [
        {"patientId": "9999", "assessmentName": "Audiometry", "status": "Pending", "tests": "Hearing"}
    ]
    
    # 2. Dump (Step 3 "Save & Trigger" beginning)
    payload_dump = {
        "tenantId": TENANT_ID,
        "templateKey": "PatientData",
        "rows": rows,
        "fileName": "test_patient_fk.csv"
    }
    
    print("Dumping PatientData records...")
    try:
        resp_dump = requests.post(f"{BASE_URL}/api/import/dump", json=payload_dump, timeout=10)
        print(f"Dump Response Status: {resp_dump.status_code}")
        if resp_dump.status_code != 200:
            print(f"Dump Failed: {resp_dump.text}")
            return
        upload_id = resp_dump.json().get("uploadId")
        print(f"Data Dumped! ID: {upload_id}")
    except Exception as e:
        print(f"Dump request failed: {e}")
        return
    
    # 3. Trigger Job
    payload_trigger = {
        "tenantId": TENANT_ID,
        "uploadIds": [upload_id]
    }
    print(f"Triggering Job for {upload_id}...")
    try:
        resp_trigger = requests.post(f"{BASE_URL}/api/import/trigger-job", json=payload_trigger, timeout=10)
        print(f"Trigger Response Status: {resp_trigger.status_code}")
        if resp_trigger.status_code != 200:
            print(f"Trigger Failed: {resp_trigger.text}")
            return
        job_id = resp_trigger.json().get("jobId")
        print(f"Job Triggered! ID: {job_id}")
    except Exception as e:
        print(f"Trigger request failed: {e}")
        return
    
    # 4. Wait for worker to process
    print("Waiting for worker to process...")
    for i in range(20):
        time.sleep(2)
        try:
            resp = requests.get(f"{BASE_URL}/api/jobs?tenantId={TENANT_ID}", timeout=10)
            jobs = resp.json().get("jobs", [])
            job = next((j for j in jobs if j["jobId"] == job_id), None)
            if job:
                status = job.get("status")
                print(f"Check {i+1}: Status={status}")
                if status in ("completed", "error"):
                    print(f"Final Job Status: {status}")
                    metrics = job.get("metrics", {})
                    print(f"Metrics: {metrics}")
                    
                    # THE CORE VERIFICATION: We expect an error because 9999 is invalid
                    if metrics.get("errors", 0) > 0:
                        print("VERIFICATION SUCCESS: Foreign Key Error was correctly caught by worker!")
                    else:
                        print("VERIFICATION FAILED: Worker did NOT catch the Foreign Key error.")
                    
                    # 5. Verify History View (api_get_data fix)
                    print("Verifying History View (api_get_data)...")
                    history_resp = requests.get(f"{BASE_URL}/api/import/data?tenantId={TENANT_ID}&jobId={job_id}", timeout=10)
                    history_data = history_resp.json().get("data", [])
                    print(f"Found {len(history_data)} history records (snapshots).")
                    
                    return job_id
            else:
                print(f"Check {i+1}: Job {job_id} not found yet in /api/jobs")
        except Exception as e:
            print(f"Poll {i+1} failed: {e}")
            
    print("Timed out waiting for job completion.")
    return None

if __name__ == "__main__":
    test_patient_job()
