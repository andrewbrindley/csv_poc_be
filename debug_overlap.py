
import sys
from db_config import get_db

db = get_db()
if db is None:
    sys.exit(1)

def check_overlap():
    print("--- CHECKING JOB OVERLAP ---")
    
    # Get last 2 jobs
    jobs = list(db.ingestion_jobs.find({"tenantId": "acme-corp"}).sort("triggeredAt", -1).limit(2))
    
    if len(jobs) < 2:
        print("Not enough jobs to compare.")
        return

    job1 = jobs[0]
    job2 = jobs[1]
    
    print(f"Job 1: {job1['jobId']} (Triggered: {job1['triggeredAt']})")
    print(f"Job 2: {job2['jobId']} (Triggered: {job2['triggeredAt']})")
    
    # Get IDs for Job 1
    recs1 = list(db.ingestion_records.find({"jobId": job1['jobId'], "status": "resolved"}, {"data.id": 1}))
    ids1 = set(r['data'].get('id') for r in recs1 if 'data' in r)
    print(f"Job 1 Processed IDs: {len(ids1)}")
    
    # Get IDs for Job 2
    recs2 = list(db.ingestion_records.find({"jobId": job2['jobId'], "status": "resolved"}, {"data.id": 1}))
    ids2 = set(r['data'].get('id') for r in recs2 if 'data' in r)
    print(f"Job 2 Processed IDs: {len(ids2)}")
    
    # Intersection
    common = ids1.intersection(ids2)
    print(f"Common IDs: {len(common)}")
    
    if len(ids1) > 0 and ids1 == ids2:
        print("CONCLUSION: Both jobs processed identical sets of IDs.")
        print("This confirms the user is re-importing the same data.")
        print("Behavior is CORRECT (Upsert updates the same entities).")
    elif len(common) == 0:
        print("CONCLUSION: Jobs processed completely disjoint sets of IDs.")
        print("If UI shows same data, there is a BUG.")
    else:
        print("CONCLUSION: Partial overlap.")

if __name__ == "__main__":
    check_overlap()
