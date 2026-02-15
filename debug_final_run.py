
import os, pymongo, json, sys
from dotenv import load_dotenv
load_dotenv()
client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client[os.getenv("MONGO_DB_NAME", "csv")]

with open("debug_final.txt", "w") as f:
    job_prefix = "13188313"
    job = db.ingestion_jobs.find_one({"jobId": {"$regex": f"^{job_prefix}"}})
    if not job:
        f.write("Job not found\n")
        # List last 10
        f.write("\nRecent Job IDs:\n")
        for j in db.ingestion_jobs.find().sort("triggeredAt", -1).limit(10):
            f.write(f"  {j.get('jobId')} | {j.get('status')}\n")
    else:
        jid = job["jobId"]
        f.write(f"JOB: {jid}\n")
        f.write(f"STATUS: {job.get('status')}\n")
        f.write(f"METRICS: {json.dumps(job.get('metrics'))}\n")
        f.write(f"TRIGGERED: {job.get('triggeredAt')}\n")
        
        recs = list(db.ingestion_records.find({"jobId": jid}))
        f.write(f"RECORDS TOTAL: {len(recs)}\n")
        for r in recs:
            f.write(f"  Row {r.get('rowIndex')}: status={r.get('status')}, error={r.get('error')}, hasProcessedData={'processedData' in r}\n")
                 
        data = list(db.tenant_data.find({"jobId": jid}))
        f.write(f"TENANT_DATA LINKED: {len(data)}\n")
        for d in data:
            f.write(f"  {d.get('templateKey')} | Op: {d.get('__operation')} | Data: {d.get('data')}\n")

        # Global search for IDs 7001/7002
        f.write("\nGLOBAL SEARCH for 7001/7002:\n")
        for d in db.tenant_data.find({"data.patientId": {"$in": ["7001", "7002", 7001, 7002]}}):
            f.write(f"  {d.get('templateKey')} | JobId: {d.get('jobId')} | Timestamp: {d.get('timestamp')}\n")
