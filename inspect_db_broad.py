from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# Connect
conn_str = os.getenv("MONGO_URI") or "mongodb://localhost:27017/"
client = MongoClient(conn_str)

dbs = client.list_database_names()
print(f"Databases: {dbs}")

for db_name in dbs:
    if db_name in ["admin", "local", "config"]:
        continue
    
    print(f"\n=== DATABASE: {db_name} ===")
    try:
        db = client[db_name]
        cols = db.list_collection_names()
        print(f"Collections: {cols}")
        
        for col_name in cols:
            print(f"  --- Collection: {col_name} ---")
            docs = list(db[col_name].find({}))
            print(f"  Count: {len(docs)}")
            for doc in docs[:5]: # Print first 5
                # Print relevant fields if they exist
                if "templateKey" in doc:
                    print(f"    Template: {doc.get('templateKey')} (Tenant: {doc.get('tenantId')})")
                elif "jobId" in doc:
                    print(f"    Job: {doc.get('jobId')}")
                else:
                    print(f"    Doc ID: {doc.get('_id')}")
    except Exception as e:
        print(f"Error inspecting {db_name}: {e}")
