
import os, pymongo, json
from dotenv import load_dotenv
load_dotenv()
client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client[os.getenv("MONGO_DB_NAME", "csv")]

job_prefix = "13188313"
job = db.ingestion_jobs.find_one({"jobId": {"$regex": f"^{job_prefix}"}})
if not job:
    print("Job not found")
else:
    jid = job["jobId"]
    print(f"JOB: {jid} | STATUS: {job['status']} | METRICS: {job.get('metrics')}")
    print(f"TRIGGERED: {job.get('triggeredAt')}")
    
    recs = list(db.ingestion_records.find({"jobId": jid}))
    print(f"RECORDS TOTAL: {len(recs)}")
    for r in recs:
        print(f"  Row {r.get('rowIndex')}: status={r.get('status')}, error={r.get('error')}, hasProcessedData={'processedData' in r}")
        if 'processedData' in r:
             print(f"    processedData: {r['processedData']}")
             
    data = list(db.tenant_data.find({"jobId": jid}))
    print(f"TENANT_DATA LINKED: {len(data)}")
    for d in data:
        print(f"  {d.get('templateKey')} | Op: {d.get('__operation')} | Data: {d.get('data')}")

    # Check for IDs 7001/7002 globally
    print("\nGLOBAL SEARCH for 7001/7002:")
    for d in db.tenant_data.find({"data.patientId": {"$in": ["7001", "7002", 7001, 7002]}}):
        print(f"  {d.get('templateKey')} | JobId: {d.get('jobId')} | Timestamp: {d.get('timestamp')}")
