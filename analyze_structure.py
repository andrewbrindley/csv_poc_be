
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

def analyze():
    print("ANALYZING TEMPLATE STRUCTURES...\n")
    
    # 1. Get System Template (Standard of Truth)
    sys_tpl = TEMPLATES.get("PatientData")
    print(f"SYSTEM TEMPLATE (PatientData): Keys={list(sys_tpl.keys())}")
    if 'fields' in sys_tpl:
        print(f"  first field keys: {list(sys_tpl['fields'][0].keys())}")
    
    # 2. Get Custom Template
    custom_tpl = db.templates.find_one({"templateKey": "notesanddocuments"})
    
    if not custom_tpl:
        print("\nERROR: Custom template 'notesanddocuments' NOT FOUND in DB.")
        return

    print(f"\nCUSTOM TEMPLATE (notesanddocuments): Keys={list(custom_tpl.keys())}")
    
    # Check for crucial keys
    required_top_keys = ['key', 'label', 'fields']
    for k in required_top_keys:
        if k not in custom_tpl and k == 'key' and 'templateKey' in custom_tpl:
            print(f"  [WARN] Missing '{k}', but has 'templateKey'. Frontend should normalize this.")
        elif k not in custom_tpl and k == 'label' and 'templateLabel' in custom_tpl:
            print(f"  [WARN] Missing '{k}', but has 'templateLabel'. Frontend should normalize this.")
        elif k not in custom_tpl:
             print(f"  [CRITICAL] Missing '{k}' at top level!")

    # Check fields
    if 'fields' in custom_tpl:
        fields = custom_tpl['fields']
        print(f"  Field count: {len(fields)}")
        if len(fields) > 0:
            f0 = fields[0]
            print(f"  First field keys: {list(f0.keys())}")
            
            # Check field integrity
            required_field_keys = ['key', 'label']
            for fk in required_field_keys:
                if fk not in f0:
                    print(f"  [CRITICAL] First field missing '{fk}'")
    else:
        print("  [CRITICAL] No 'fields' array found!")

    # Check for `_id` and remove for clean print
    custom_tpl['_id'] = str(custom_tpl['_id'])
    
    print("\nFULL DUMP of Custom Template:")
    print(json_util.dumps(custom_tpl, indent=2))

if __name__ == "__main__":
    analyze()
