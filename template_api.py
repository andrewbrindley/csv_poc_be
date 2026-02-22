
from flask import Blueprint, request, jsonify
from datetime import datetime
import re

from db_config import get_templates_collection, get_tenant_data_collection
from core_config import TEMPLATES
from auth_utils import get_current_user_id
from audit_logger import log_audit_event

api_template_bp = Blueprint('api_template_bp', __name__)

@api_template_bp.route("/templates", methods=["GET"])
def api_get_templates():
    """
    Get all templates for a tenant (custom + defaults).
    ---
    tags:
      - Templates
    parameters:
      - in: query
        name: tenantId
        type: string
        required: true
        description: The ID of the tenant.
    responses:
      200:
        description: A list of templates.
    """
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400
    
    coll_templates = get_templates_collection()
    if coll_templates is None:
        # If DB not connected, return only hardcoded templates
        return jsonify({"templates": list(TEMPLATES.values())})
    
    # Fetch custom templates from DB
    custom_templates = list(coll_templates.find({"tenantId": tenant_id}))
    
    # Convert ObjectId to string for JSON serialization
    for tpl in custom_templates:
        tpl["_id"] = str(tpl["_id"])
        if "createdAt" in tpl:
            tpl["createdAt"] = tpl["createdAt"].isoformat() + "Z"
        if "updatedAt" in tpl:
            tpl["updatedAt"] = tpl["updatedAt"].isoformat() + "Z"
    
    # Merge with hardcoded templates (custom templates override)
    all_templates = {**TEMPLATES}
    for tpl in custom_templates:
        all_templates[tpl["templateKey"]] = tpl
    
    return jsonify({"templates": list(all_templates.values())})


@api_template_bp.route("/templates", methods=["POST"])
def api_create_template():
    """
    Create a new custom template.
    ---
    tags:
      - Templates
    parameters:
      - in: body
        name: body
        schema:
          type: object
        required: true
        description: Template metadata and fields.
    responses:
      201:
        description: Template created successfully.
      400:
        description: Invalid request.
    """
    body = request.get_json(force=True, silent=False) or {}
    tenant_id = body.get("tenantId")
    template_key = body.get("templateKey")
    
    if not tenant_id or not template_key:
        return jsonify({"error": "tenantId and templateKey are required"}), 400
    
    # Validate template key (alphanumeric + underscores only)
    if not re.match(r'^[a-zA-Z0-9_]+$', template_key):
        return jsonify({"error": "templateKey must be alphanumeric with underscores only"}), 400
    
    coll_templates = get_templates_collection()
    if coll_templates is None:
        return jsonify({"error": "MongoDB not connected"}), 500
    
    # Check if template already exists
    existing = coll_templates.find_one({"tenantId": tenant_id, "templateKey": template_key})
    if existing:
        return jsonify({"error": f"Template '{template_key}' already exists"}), 409
    
    # Validate at least 1 field
    fields = body.get("fields", [])
    if not fields or len(fields) == 0:
        return jsonify({"error": "At least 1 field is required"}), 400
    
    # Validate at least 1 identifier field
    has_identifier = any(f.get("identifier") for f in fields)
    if not has_identifier:
        return jsonify({"error": "At least 1 identifier field is required"}), 400
    
    # Create template document
    template_doc = {
        "tenantId": tenant_id,
        "templateKey": template_key,
        "templateLabel": body.get("templateLabel", template_key),
        "keywords": body.get("keywords", []),
        "dependencies": body.get("dependencies", []),
        "references": body.get("references", {}),
        "fields": fields,
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
        "createdBy": body.get("createdBy", "unknown")
    }
    
    result = coll_templates.insert_one(template_doc)
    template_doc["_id"] = str(result.inserted_id)
    template_doc["createdAt"] = template_doc["createdAt"].isoformat() + "Z"
    template_doc["updatedAt"] = template_doc["updatedAt"].isoformat() + "Z"
    
    log_audit_event(tenant_id, get_current_user_id(), "TEMPLATE_CREATED", template_doc["_id"], {"templateKey": template_key})
    
    return jsonify({"message": "Template created successfully", "template": template_doc}), 201


@api_template_bp.route("/templates/<template_key>", methods=["GET"])
def api_get_template(template_key):
    """
    Get a single template by key.
    ---
    tags:
      - Templates
    parameters:
      - in: path
        name: template_key
        type: string
        required: true
        description: The key of the template.
      - in: query
        name: tenantId
        type: string
        required: true
        description: The ID of the tenant.
    responses:
      200:
        description: The requested template.
      404:
        description: Template not found.
    """
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400
    
    coll_templates = get_templates_collection()
    if coll_templates is None:
        # Check hardcoded templates
        if template_key in TEMPLATES:
            return jsonify({"template": TEMPLATES[template_key]})
        return jsonify({"error": "Template not found"}), 404
    
    # Check custom templates first
    template = coll_templates.find_one({"tenantId": tenant_id, "templateKey": template_key})
    if template:
        template["_id"] = str(template["_id"])
        if "createdAt" in template:
            template["createdAt"] = template["createdAt"].isoformat() + "Z"
        if "updatedAt" in template:
            template["updatedAt"] = template["updatedAt"].isoformat() + "Z"
        return jsonify({"template": template})
    
    # Fallback to hardcoded templates
    if template_key in TEMPLATES:
        return jsonify({"template": TEMPLATES[template_key]})
    
    return jsonify({"error": "Template not found"}), 404


@api_template_bp.route("/templates/<template_key>", methods=["PUT"])
def api_update_template(template_key):
    """Update an existing template."""
    body = request.get_json(force=True, silent=False) or {}
    tenant_id = body.get("tenantId")
    
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400
    
    coll_templates = get_templates_collection()
    if coll_templates is None:
        return jsonify({"error": "MongoDB not connected"}), 500
    
    # Check if template exists
    existing = coll_templates.find_one({"tenantId": tenant_id, "templateKey": template_key})
    if not existing:
        return jsonify({"error": f"Template '{template_key}' not found"}), 404
    
    # Validate fields if provided
    fields = body.get("fields")
    if fields is not None:
        if len(fields) == 0:
            return jsonify({"error": "At least 1 field is required"}), 400
        has_identifier = any(f.get("identifier") for f in fields)
        if not has_identifier:
            return jsonify({"error": "At least 1 identifier field is required"}), 400
    
    # Update template
    update_data = {
        "updatedAt": datetime.utcnow()
    }
    
    if "templateLabel" in body:
        update_data["templateLabel"] = body["templateLabel"]
    if "keywords" in body:
        update_data["keywords"] = body["keywords"]
    if "dependencies" in body:
        update_data["dependencies"] = body["dependencies"]
    if "references" in body:
        update_data["references"] = body["references"]
    if "fields" in body:
        update_data["fields"] = body["fields"]
    
    coll_templates.update_one(
        {"tenantId": tenant_id, "templateKey": template_key},
        {"$set": update_data}
    )
    
    # Fetch updated template
    updated = coll_templates.find_one({"tenantId": tenant_id, "templateKey": template_key})
    updated["_id"] = str(updated["_id"])
    if "createdAt" in updated:
        updated["createdAt"] = updated["createdAt"].isoformat() + "Z"
    if "updatedAt" in updated:
        updated["updatedAt"] = updated["updatedAt"].isoformat() + "Z"
    
    log_audit_event(tenant_id, get_current_user_id(), "TEMPLATE_UPDATED", updated["_id"], {"templateKey": template_key})
        
    return jsonify({"message": "Template updated successfully", "template": updated})


@api_template_bp.route("/templates/<template_key>", methods=["DELETE"])
def api_delete_template(template_key):
    """Delete a template (with validation)."""
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400
    
    coll_templates = get_templates_collection()
    if coll_templates is None:
        return jsonify({"error": "MongoDB not connected"}), 500
    
    # Check if template exists
    existing = coll_templates.find_one({"tenantId": tenant_id, "templateKey": template_key})
    if not existing:
        return jsonify({"error": f"Template '{template_key}' not found"}), 404
    
    # Check if data exists for this template

    coll_data = get_tenant_data_collection()
    if coll_data is not None:
        data_count = coll_data.count_documents({"tenantId": tenant_id, "templateKey": template_key})
        if data_count > 0:
            return jsonify({
                "error": f"Cannot delete template '{template_key}' because {data_count} records exist. Delete the data first."
            }), 409
    
    target_id = str(existing["_id"])
    # Delete template
    coll_templates.delete_one({"tenantId": tenant_id, "templateKey": template_key})
    
    log_audit_event(tenant_id, get_current_user_id(), "TEMPLATE_DELETED", target_id, {"templateKey": template_key})
    
    return jsonify({"message": f"Template '{template_key}' deleted successfully"})
