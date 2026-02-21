"""
auth_utils.py

Authentication and Authorization utilities for the CSV Data Import Tool.
Provides decorators to enforce RBAC based on the X-User-ID header.
"""

from functools import wraps
from flask import request, jsonify
from db_config import get_users_collection

# Roles
ROLE_ADMIN = "ADMIN"
ROLE_EDITOR = "EDITOR"
ROLE_VIEWER = "VIEWER"

# Global Role Hierarchy (for easier checking)
# ADMIN can do everything
# EDITOR can do EDITOR and VIEWER things
# VIEWER can only do VIEWER things
ROLE_HIERARCHY = {
    ROLE_ADMIN: 3,
    ROLE_EDITOR: 2,
    ROLE_VIEWER: 1,
}

def get_current_user_id():
    """Helper to extract user ID from headers."""
    return request.headers.get("X-User-ID")

def get_user_from_db(user_id):
    """Fetch user document from MongoDB."""
    if not user_id:
        return None
    coll = get_users_collection()
    if coll is None:
        return None
    return coll.find_one({"userId": user_id})

def get_user_role_for_tenant(user, tenant_id):
    """
    Extract the role for a specific tenant from the user document.
    Returns the role name (str) or None if not found.
    """
    if not user or not tenant_id:
        return None
    
    tenants = user.get("tenants", [])
    for t in tenants:
        if t.get("tenantId") == tenant_id:
            return t.get("role")
    
    return None

def require_auth(f):
    """Decorator to ensure the X-User-ID header is present and the user exists."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"error": "Authentication required (missing X-User-ID header)"}), 401
        
        user = get_user_from_db(user_id)
        if not user:
            return jsonify({"error": "User not found"}), 401
        
        # Attach user to context for use in the route if needed
        request.user = user
        return f(*args, **kwargs)
    return decorated_function

def require_role(required_roles):
    """
    Decorator to ensure the user has one of the required roles for the active tenant.
    Works for routes that have tenantId in query params or request body.
    """
    if isinstance(required_roles, str):
        required_roles = [required_roles]

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user_id = get_current_user_id()
            if not user_id:
                return jsonify({"error": "Authentication required"}), 401
            
            user = get_user_from_db(user_id)
            if not user:
                return jsonify({"error": "User not found"}), 401
            
            # Extract tenantId from context (path kwargs, query, or body)
            tenant_id = kwargs.get("tenant_id") or request.args.get("tenantId")
            if not tenant_id and request.is_json:
                tenant_id = request.get_json(force=True, silent=True).get("tenantId")
            
            if not tenant_id:
                return jsonify({"error": "tenantId context required for role check"}), 400

            user_role = get_user_role_for_tenant(user, tenant_id)
            
            # Global Super Admin Bypass
            if user_id == "admin-1":
                user_role = ROLE_ADMIN
            
            if not user_role:
                return jsonify({"error": f"User has no access to tenant '{tenant_id}'"}), 403
            
            user_level = ROLE_HIERARCHY.get(user_role, 0)
            highest_required_level = max([ROLE_HIERARCHY.get(r, 0) for r in required_roles])
            
            if user_level < highest_required_level:
                return jsonify({"error": "Forbidden: Insufficient permissions"}), 403

            request.user = user
            request.user_role = user_role
            return f(*args, **kwargs)
        return decorated_function
    return decorator
