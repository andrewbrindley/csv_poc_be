
import os
import json
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util
# Add current directory to path so we can import core_config
sys.path.append(os.getcwd())
from core_config import TEMPLATES

load_dotenv()
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(mongo_uri)
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def compare():
    print("--- System Template (PatientData) ---")
    sys_tpl = TEMPLATES.get("PatientData")
    if sys_tpl:
        print(json.dumps(sys_tpl, indent=2, default=str))
    else:
        print("System template PatientData not found in TEMPLATES")

    print("\n--- Custom Template (notesanddocuments) ---")
    custom_tpl = db.templates.find_one({"templateKey": "notesanddocuments"})
    
    if custom_tpl:
        # Normalize _id
        custom_tpl["_id"] = str(custom_tpl["_id"])
        print(json_util.dumps(custom_tpl, indent=2))
    else:
        print("Custom template notesanddocuments NOT FOUND in DB")

if __name__ == "__main__":
    compare()
