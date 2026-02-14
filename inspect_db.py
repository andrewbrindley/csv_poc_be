from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

conn_str = os.getenv("MONGO_URI") or "mongodb://localhost:27017/"
client = MongoClient(conn_str)
db = client["csv"] 
coll_data = db["tenant_data"]
coll_templates = db["templates"]


def check_tenant(tenant_id):
    print(f"\n--- Data Counts for {tenant_id} ---")
    vals = ["People", "Bookings", "PatientData"]
    for v in vals:
        c = coll_data.count_documents({"tenantId": tenant_id, "templateKey": v})
        print(f"TemplateKey: {v} -> Count: {c}")
        
    print(f"Total Raw Uploads: {db['raw_uploads'].count_documents({'tenantId': tenant_id})}")

check_tenant("acme-corp")
check_tenant("demo-tenant")
