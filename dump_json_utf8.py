
import os
import json
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util

load_dotenv()
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def dump():
    custom_tpl = db.templates.find_one({"templateKey": "notesanddocuments"})
    
    if custom_tpl:
        # Normalize
        custom_tpl['_id'] = str(custom_tpl['_id'])
        with open('structure_utf8.json', 'w', encoding='utf-8') as f:
            f.write(json_util.dumps(custom_tpl, indent=2))
        print("Done writing structure_utf8.json")
    else:
        print("Template not found")

if __name__ == "__main__":
    dump()
