
from flask import Blueprint, request, jsonify
from datetime import datetime
from db_config import get_tenant_data_collection, get_templates_collection
from bson import ObjectId
import re

# Fallback templates from main (circular import avoidance: we might need to refactor if main imports this)
# Better to pass distinct templates or import from a shared module.
# For now, let's assume we can import TEMPLATES if we are careful, or just duplicate/pass it.
# Actually, main.py imports this, so importing main here will cause circular import.
# We should move TEMPLATES to a shared file or just rely on DB + hardcoded fallback here.
# Let's define the hardcoded fallback here to avoid circular dependency.

PEOPLE_STATUSES = ["Candidate", "Employee"]
BOOKING_STATUSES = ["Pending", "Booked", "Completed", "Cancelled"]
PATIENT_STATUSES = ["Pending", "In Progress", "Completed", "Cancelled"]

# Minimal fallback for core types if DB is empty
FALLBACK_TEMPLATES = {
    "People": {"key": "People", "fields": []},
    "Bookings": {"key": "Bookings", "fields": []},
    "PatientData": {"key": "PatientData", "fields": []}
}

data_bp = Blueprint('data_bp', __name__)

def resolve_template_key(tenant_id, resource_name):
    """
    Map resource name (URL slug) to Template Key (DB).
    e.g. "people" -> "People", "bookings" -> "Bookings"
    """
    # 1. Try case-insensitive match against hardcoded fallbacks
    for key in FALLBACK_TEMPLATES.keys():
        if key.lower() == resource_name.lower():
            return key, FALLBACK_TEMPLATES[key]
    
    # 2. Try DB templates
    coll_templates = get_templates_collection()
    if coll_templates:
        # Case-insensitive search using regex
        # This allows "custom_form" to match "Custom_Form"
        tpl = coll_templates.find_one({
            "tenantId": tenant_id, 
            "templateKey": {"$regex":f"^{re.escape(resource_name)}$", "$options": "i"}
        })
        if tpl:
            return tpl["templateKey"], tpl

    return None, None

@data_bp.route("/<tenant_id>/<resource_name>", methods=["GET"])
def list_resources(tenant_id, resource_name):
    template_key, tpl_def = resolve_template_key(tenant_id, resource_name)
    if not template_key:
        return jsonify({"error": f"Resource '{resource_name}' not found"}), 404

    coll_data = get_tenant_data_collection()
    if coll_data is None:
        return jsonify({"error": "Database not available"}), 500

    try:
        limit = int(request.args.get("limit", 50))
        skip = int(request.args.get("skip", 0))
    except ValueError:
        limit = 50
        skip = 0

    query = {
        "tenantId": tenant_id,
        # Use case-insensitive match for templateKey to handle mapped vs legacy data
        "templateKey": {"$regex":f"^{re.escape(template_key)}$", "$options": "i"}
    }

    # Filtering
    reserved_params = {"limit", "skip", "sort", "fields"}
    for k, v in request.args.items():
        if k not in reserved_params:
            query[f"data.{k}"] = v

    # Sorting
    sort_param = request.args.get("sort")
    sort_criteria = [("timestamp", -1)]
    if sort_param:
        try:
            field, direction = sort_param.split(":")
            mongo_dir = 1 if direction.lower() == "asc" else -1
            if field not in ["timestamp", "_id", "jobId"]:
                field = f"data.{field}"
            sort_criteria = [(field, mongo_dir)]
        except:
            pass

    total = coll_data.count_documents(query)
    cursor = coll_data.find(query).sort(sort_criteria).skip(skip).limit(limit)

    results = []
    for doc in cursor:
        # Flatten structure: id, metadata, ...data
        item = doc.get("data", {})
        item["id"] = str(doc["_id"])
        item["_metadata"] = {
            "createdAt": doc.get("timestamp"),
            "jobId": doc.get("jobId"),
            "operation": doc.get("operation")
        }
        
        if isinstance(item["_metadata"]["createdAt"], datetime):
            item["_metadata"]["createdAt"] = item["_metadata"]["createdAt"].isoformat() + "Z"
            
        results.append(item)

    return jsonify({
        "object": "list",
        "data": results,
        "meta": {
            "total": total,
            "skip": skip,
            "limit": limit,
            "resource": resource_name
        }
    })

@data_bp.route("/<tenant_id>/<resource_name>/<id>", methods=["GET"])
def get_resource(tenant_id, resource_name, id):
    template_key, _ = resolve_template_key(tenant_id, resource_name)
    if not template_key:
        return jsonify({"error": f"Resource '{resource_name}' not found"}), 404

    coll_data = get_tenant_data_collection()
    
    # Try Mongo ID
    try:
        oid = ObjectId(id)
        doc = coll_data.find_one({"_id": oid, "tenantId": tenant_id, "templateKey": template_key})
    except:
        doc = None

    # Try Business ID (fallback)
    if not doc:
        doc = coll_data.find_one({
            "tenantId": tenant_id, 
            "templateKey": template_key,
            "data.id": id
        })

    if not doc:
        return jsonify({"error": "Resource not found"}), 404

    item = doc.get("data", {})
    item["id"] = str(doc["_id"])
    item["_metadata"] = {
        "createdAt": doc.get("timestamp"),
        "jobId": doc.get("jobId")
    }
    if isinstance(item["_metadata"]["createdAt"], datetime):
        item["_metadata"]["createdAt"] = item["_metadata"]["createdAt"].isoformat() + "Z"

    return jsonify(item)

@data_bp.route("/<tenant_id>/<resource_name>", methods=["POST"])
def create_resource(tenant_id, resource_name):
    template_key, tpl_def = resolve_template_key(tenant_id, resource_name)
    if not template_key:
        return jsonify({"error": f"Resource '{resource_name}' not found"}), 404

    data = request.get_json() or {}
    
    coll_data = get_tenant_data_collection()
    
    new_doc = {
        "tenantId": tenant_id,
        "templateKey": template_key,
        "data": data,
        "timestamp": datetime.utcnow(),
        "source": "api",
        "__operation": "created"
    }
    
    res = coll_data.insert_one(new_doc)
    
    return jsonify({
        "id": str(res.inserted_id),
        "message": "Resource created",
        "link": f"/api/v1/{tenant_id}/{resource_name}/{str(res.inserted_id)}"
    }), 201

