from datetime import datetime
from db_config import get_audit_logs_collection

def log_audit_event(tenant_id, user_id, action, target_id=None, details=None):
    coll = get_audit_logs_collection()
    if coll is not None:
        coll.insert_one({
            "tenantId": tenant_id,
            "userId": user_id,
            "action": action,
            "targetId": target_id,
            "details": details or {},
            "timestamp": datetime.utcnow()
        })
