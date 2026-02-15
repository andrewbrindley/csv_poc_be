
import pymongo
import os
from dotenv import load_dotenv
load_dotenv()

client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def check_db():
    print("Latest Ingestion Jobs:")
    for job in db.ingestion_jobs.find().sort("triggeredAt", -1).limit(3):
        print(f"JobId: {job.get('_id')}, Status: {job.get('status')}, Metrics: {job.get('metrics')}")
    
    print("\nLatest PatientData Records:")
    for rec in db.ingestion_records.find({"templateKey": "PatientData"}).sort("_id", -1).limit(5):
        print(f"RecordId: {rec.get('_id')}, Status: {rec.get('status')}, Error: {rec.get('error')}, Data: {rec.get('data')}")

if __name__ == "__main__":
    check_db()
