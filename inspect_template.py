
import os
from pymongo import MongoClient
from bson.json_util import dumps

client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
db = client[os.getenv("MONGO_DB_NAME", "csv")]

def inspect_template():
    tpl = db.templates.find_one({"templateKey": "notesanddocuments"})
    print(dumps(tpl, indent=2))

if __name__ == "__main__":
    inspect_template()
