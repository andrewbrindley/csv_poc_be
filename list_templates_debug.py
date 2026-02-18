
import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util

load_dotenv()
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def list_templates():
    print("Listing all templates for 'acme-corp':")
    templates = list(db.templates.find({"tenantId": "acme-corp"}))
    for t in templates:
        print(f"Key: {t.get('templateKey', 'N/A')}, Label: {t.get('templateLabel', 'N/A')}")
        print(f"  Top-level keys: {list(t.keys())}")
        if 'fields' in t:
             print(f"  Field count: {len(t['fields'])}")
        else:
             print("  [WARN] No fields array")
        print("-" * 20)

if __name__ == "__main__":
    list_templates()
