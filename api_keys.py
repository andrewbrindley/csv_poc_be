import secrets
import string
import hashlib
from datetime import datetime
from db_config import get_db

def _get_api_keys_collection():
    db = get_db()
    if db is not None:
        return db.api_keys
    return None

def generate_api_key(prefix="sk_live_"):
    """Generate a secure API key with a prefix."""
    random_str = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
    return f"{prefix}{random_str}"

def hash_api_key(api_key):
    """Create a secure hash of the API key for database storage."""
    return hashlib.sha256(api_key.encode('utf-8')).hexdigest()

def create_api_key(tenant_id, user_id, name):
    """Generate and store a new API key."""
    coll = _get_api_keys_collection()
    if coll is None:
        return None, "Database not available"
        
    raw_key = generate_api_key()
    hashed_key = hash_api_key(raw_key)
    
    # Store only the hash, plus meta info. 
    # Use the first 8 chars of raw key as a preview.
    preview = raw_key[:12] + "..."
    
    doc = {
        "tenantId": tenant_id,
        "userId": user_id,
        "name": name,
        "keyHash": hashed_key,
        "preview": preview,
        "createdAt": datetime.utcnow(),
        "lastUsedAt": None,
        "status": "active"
    }
    res = coll.insert_one(doc)
    
    # Return raw key ONLY NOW. We don't save it.
    doc["_id"] = str(res.inserted_id)
    doc["rawKey"] = raw_key
    
    return doc, None

def list_api_keys(tenant_id):
    """List all API keys for a tenant (without raw secrets)."""
    coll = _get_api_keys_collection()
    if coll is None:
        return []
        
    keys = list(coll.find({"tenantId": tenant_id, "status": "active"}).sort("createdAt", -1))
    for k in keys:
        k["_id"] = str(k["_id"])
        if "createdAt" in k:
            k["createdAt"] = k["createdAt"].isoformat() + "Z"
        if "lastUsedAt" in k and k["lastUsedAt"]:
            k["lastUsedAt"] = k["lastUsedAt"].isoformat() + "Z"
        # Never return the hash to the frontend either, just the preview
        k.pop("keyHash", None)
        
    return keys

def revoke_api_key(tenant_id, key_id):
    """Mark an API key as revoked."""
    from bson import ObjectId
    coll = _get_api_keys_collection()
    if coll is None:
        return False
        
    try:
        oid = ObjectId(key_id)
    except:
        return False
        
    res = coll.update_one(
        {"_id": oid, "tenantId": tenant_id},
        {"$set": {"status": "revoked"}}
    )
    return res.modified_count > 0

def authenticate_api_key(raw_key):
    """Verify an API key and return the associated tenant_id/user_id."""
    coll = _get_api_keys_collection()
    if coll is None:
        return None
        
    hashed_key = hash_api_key(raw_key)
    doc = coll.find_one({"keyHash": hashed_key, "status": "active"})
    if doc:
        # Update last used
        coll.update_one({"_id": doc["_id"]}, {"$set": {"lastUsedAt": datetime.utcnow()}})
        return {
            "tenantId": doc["tenantId"],
            "userId": doc["userId"],
            "name": doc["name"]
        }
    return None
