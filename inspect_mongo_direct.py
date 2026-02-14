import os
import pymongo
from dotenv import load_dotenv

# Load .env
base_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "csv")

try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    print(f"Connected to DB: {MONGO_DB_NAME}")
    
    # List collections
    collections = db.list_collection_names()
    print(f"Collections: {collections}")
    
    tenant_data = db["tenant_data"]
    
    # Count total documents
    total = tenant_data.count_documents({})
    print(f"Total documents in tenant_data: {total}")
    
    # Group by tenantId
    pipeline = [
        {"$group": {"_id": "$tenantId", "count": {"$sum": 1}}}
    ]
    print("\nCounts by Tenant:")
    for doc in tenant_data.aggregate(pipeline):
        print(f"  {doc['_id']}: {doc['count']}")

    # Detail for 'acme-corp'
    print("\nDetail for 'acme-corp':")
    acme_pipeline = [
        {"$match": {"tenantId": "acme-corp"}},
        {"$group": {"_id": "$templateKey", "count": {"$sum": 1}}}
    ]
    for doc in tenant_data.aggregate(acme_pipeline):
        print(f"  Template '{doc['_id']}': {doc['count']}")

    # Sample a few docs for 'acme-corp' to check structure
    print("\nSample Docs (acme-corp):")
    for doc in tenant_data.find({"tenantId": "acme-corp"}).limit(3):
        # print specific fields to be concise
        print(f"  ID: {doc.get('_id')}, Key: {doc.get('templateKey')}, Data.keys: {list(doc.get('data', {}).keys())}")

    print("\n--- Ingestion Jobs (acme-corp) ---")
    jobs = db["ingestion_jobs"]
    for job in jobs.find({"tenantId": "acme-corp"}):
        print(f"Job: {job.get('_id')}, Status: {job.get('status')}, File: {job.get('fileName')}, Templates: {job.get('templateKeys')}")

    print("\n--- Ingestion Records (acme-corp) ---")
    records = db["ingestion_records"]
    # Group by template and status
    rec_pipeline = [
        {"$match": {"tenantId": "acme-corp"}},
        {"$group": {"_id": {"template": "$templateKey", "status": "$status"}, "count": {"$sum": 1}}}
    ]
    print("\n--- ID Uniqueness Check (People) ---")
    people_ids = []
    for doc in records.find({"tenantId": "acme-corp", "templateKey": "People"}):
        data = doc.get("data", {})
        # Assuming 'id' is the identifier for People
        pid = data.get("id")
        if pid:
            people_ids.append(pid)
            
    print(f"Total People IDs found: {len(people_ids)}")
    unique_ids_ingestion = sorted(list(set(people_ids)))
    print(f"Unique People IDs (Ingestion): {len(unique_ids_ingestion)}")
    print(f"IDs: {unique_ids_ingestion}")

    print("\n--- IDs in Tenant Data (People) ---")
    tenant_ids = []
    for doc in tenant_data.find({"tenantId": "acme-corp", "templateKey": "People"}):
        # The ID is inside 'data.id'
        tid = doc.get("data", {}).get("id")
        tenant_ids.append(tid)
    
    unique_ids_tenant = sorted(list(set(tenant_ids)))
    print(f"IDs in Tenant Data: {len(unique_ids_tenant)}")
    print(f"IDs: {unique_ids_tenant}")
    
    missing = [x for x in unique_ids_ingestion if x not in unique_ids_tenant]
    print(f"\nMissing IDs: {missing}")

    print("\n--- Detailed Analysis: Missing (1001) vs Present (1002) ---")
    
    rec_missing = records.find_one({"tenantId": "acme-corp", "templateKey": "People", "data.id": "1001"})
    if rec_missing:
        print(f"Missing Record (1001): Job={rec_missing.get('jobId')}, Status={rec_missing.get('status')}")
    else:
        print("Missing Record (1001) not found in ingestion_records!")

    rec_present = records.find_one({"tenantId": "acme-corp", "templateKey": "People", "data.id": "1002"})
    if rec_present:
        print(f"Present Record (1002): Job={rec_present.get('jobId')}, Status={rec_present.get('status')}")
    else:
        print("Present Record (1002) not found in ingestion_records!")
        
    # Check if they look different structure-wise
    print("\n--- Job Details ---")
    
    missing_job_id = "e6bbb8b5-7cf1-40aa-a27b-7c862339521f"
    present_job_id = "a1d7afbe-efcf-437b-8b61-c2e87d9f88c9"
    
    j_miss = jobs.find_one({"_id": missing_job_id})  # ID is string in jobs? or ObjectId? 
    # Usually jobs have _id as ObjectId but user code usually stores string jobId separately or uses _id as string. 
    # Looking at logs: "Job: 698..." looks like ObjectId hex.
    # But detailed analysis printed: "Job=e6bbb..." looks like UUID string.
    # Check 'jobId' field vs '_id'.
    
    j_miss = jobs.find_one({"jobId": missing_job_id})
    print(f"Job (Missing) {missing_job_id}: {j_miss}")

    print("\n--- Tenant Data Provenance ---")
    for doc in tenant_data.find({"tenantId": "acme-corp", "templateKey": "People"}):
        print(f"ID: {doc.get('data', {}).get('id')}, JobID: {doc.get('jobId')}, Timestamp: {doc.get('timestamp')}")

    print("\n--- Job C Analysis (Source of current data) ---")
    job_c_id = "03f6a5b6-f39f-4a7f-a3eb-4dd20131eb0a"
    job_c = jobs.find_one({"jobId": job_c_id})
    print(f"Job C: {job_c}")

    j_pres = jobs.find_one({"jobId": present_job_id})
    print(f"Job (Present) {present_job_id}: {j_pres}")
        
except Exception as e:
    print(f"Error: {e}")
