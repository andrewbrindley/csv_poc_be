
import sys
import json
import pymongo
from bson.json_util import dumps

sys.stdout.reconfigure(encoding='utf-8')

try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["csv"]
    tpl = db.templates.find_one({"templateKey": "notesanddocuments"})
    
    if tpl:
        # Convert ObjectId
        tpl["_id"] = str(tpl["_id"])
        print(json.dumps(tpl, indent=2, default=str))
    else:
        print("TEMPLATE_NOT_FOUND")
        
except Exception as e:
    print(f"ERROR: {e}")
