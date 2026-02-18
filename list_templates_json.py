
import os
import sys
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util

load_dotenv()
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def list_templates():
    templates = list(db.templates.find({"tenantId": "acme-corp"}))
    for t in templates:
        t['_id'] = str(t['_id'])
        if 'createdAt' in t: t['createdAt'] = str(t['createdAt'])
        if 'updatedAt' in t: t['updatedAt'] = str(t['updatedAt'])
        
    with open('templates_list.json', 'w', encoding='utf-8') as f:
        f.write(json_util.dumps(templates, indent=2))
    print("Done writing templates_list.json")

if __name__ == "__main__":
    list_templates()
