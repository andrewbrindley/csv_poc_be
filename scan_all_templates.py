
import requests
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def scan_templates():
    print("Scanning all templates in DB...")
    templates = list(db.templates.find())
    
    tenant_map = {}
    for t in templates:
        tid = t.get("tenantId", "UNKNOWN")
        if tid not in tenant_map:
            tenant_map[tid] = []
        tenant_map[tid].append(t.get("templateKey"))
        
    for tid, keys in tenant_map.items():
        print(f"Tenant: '{tid}' has {len(keys)} custom templates: {keys}")
        
    if not tenant_map:
        print("No custom templates found in DB.")

if __name__ == "__main__":
    scan_templates()
