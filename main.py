from __future__ import annotations
"""
Single-file backend for the CSV Data Import Tool (pruned, no DB).

Features:
- Flask backend (stateless: no DB persistence)
- Templates: People, Bookings, PatientData
- PII-aware field metadata (used for cleaning, not logging)
- Algorithmic header mapping
- Algorithmic value cleaning & validation
- Optional GPT usage for advanced cleaning (if OPENAI_API_KEY set and useAi=True)
- End-to-end preview endpoint that keeps almost all logic in Flask
- Schema-driven AI cleaning prompt (port of buildCleanPrompt)
"""

import os
import json
import re
import csv
from datetime import datetime
import sys
from dotenv import load_dotenv

load_dotenv()

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from flasgger import Swagger
import uuid
import threading
import time
import hashlib
import random
from db_config import (
    get_imports_collection, 
    get_tenant_data_collection, 
    get_raw_uploads_collection, 
    get_ingestion_jobs_collection, 
    get_ingestion_records_collection, 
    get_templates_collection, 
    get_webhooks_collection,
    get_users_collection,
    get_header_aliases_collection,
    get_api_keys_collection,
    get_audit_logs_collection,
)
from api_keys import create_api_key, list_api_keys, revoke_api_key
from auth_utils import (
    require_auth, 
    require_role, 
    ROLE_ADMIN, 
    ROLE_EDITOR, 
    ROLE_VIEWER,
    get_current_user_id
)

def seed_users():
    """Seed default users for testing RBAC if the collection is empty."""
    coll = get_users_collection()
    if coll is None:
        return
    
    if coll.count_documents({}) == 0:
        print("AUTH: Seeding default users...")
        users = [
            {
                "userId": "admin-1",
                "name": "Global Admin",
                "tenants": [
                    {"tenantId": "acme-corp", "role": ROLE_ADMIN},
                    {"tenantId": "globex", "role": ROLE_ADMIN},
                    {"tenantId": "stark-ind", "role": ROLE_ADMIN},
                ]
            },
            {
                "userId": "editor-1",
                "name": "Acme Editor",
                "tenants": [
                    {"tenantId": "acme-corp", "role": ROLE_EDITOR}
                ]
            },
            {
                "userId": "viewer-1",
                "name": "Acme Viewer",
                "tenants": [
                    {"tenantId": "acme-corp", "role": ROLE_VIEWER}
                ]
            }
        ]
        coll.insert_many(users)
        print(f"AUTH: Inserted {len(users)} default users.")

# Seed on import
seed_users()


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
# IMPORTANT: Do NOT hardcode secrets. Set OPENAI_API_KEY in your environment.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

app = Flask(__name__)
CORS(app, supports_credentials=True, resources={r"/api/*": {"origins": "*"}})

# Initialize Swagger with a custom template to avoid default /tos
swagger_template = {
    "info": {
        "title": "CSV Data Import API",
        "description": "API documentation for the CSV data importing tool.",
        "version": "1.0.0"
    },
    "securityDefinitions": {
        "Bearer": {
            "type": "apiKey",
            "name": "Authorization",
            "in": "header",
            "description": "Enter the token with the 'Bearer: ' prefix, e.g. 'Bearer sk_live_abc123'."
        }
    },
    "security": [
        {
            "Bearer": []
        }
    ]
}
swagger = Swagger(app, template=swagger_template)

@app.route('/favicon.ico')
def favicon():
    return '', 204


# Register REST API Blueprint
try:
    from api_data import data_bp
    app.register_blueprint(data_bp, url_prefix="/api/v1")
    print("Main: Registered data_bp at /api/v1")
except Exception as e:
    print(f"Main: Failed to register data_bp: {e}")

# Register Template API Blueprint
try:
    from template_api import api_template_bp
    app.register_blueprint(api_template_bp, url_prefix="/api")
    print("Main: Registered api_template_bp at /api")
except Exception as e:
    print(f"Main: Failed to register api_template_bp: {e}")

# Register GraphQL Endpoint
try:
    from flask_graphql import GraphQLView
    from schema_builder import build_schema
    from db_config import get_templates_collection
    from core_config import TEMPLATES as DEFAULT_TEMPLATES
    
    # Custom View to inject tenant_id from query params into context
    class CustomGraphQLView(GraphQLView):
        def get_context(self):
            # Return a dict so we can inject values. 
            # info.context in resolvers will be this dict.
            return {
                'request': request,
                'tenant_id': request.args.get('tenantId')
            }

    @app.route('/graphql', methods=['GET', 'POST'])
    def graphql_endpoint():
        tenant_id = request.args.get('tenantId')
        if not tenant_id:
            # If no tenant is provided, just use the defaults so the playground still loads
            all_templates = {**DEFAULT_TEMPLATES}
        else:
            all_templates = {**DEFAULT_TEMPLATES}
            coll = get_templates_collection()
            if coll is not None:
                custom = list(coll.find({"tenantId": tenant_id}))
                for t in custom:
                    all_templates[t["templateKey"]] = t
                    
        # Build schema for this tenant dynamically
        schema = build_schema(all_templates)
        
        graphql_view = CustomGraphQLView.as_view(
            f'graphql_view_{tenant_id or "default"}',
            schema=schema,
            graphiql=True # Enable GraphiQL interface
        )
        return graphql_view()
        
    print("Main: Registered GraphQL at /graphql")
except Exception as e:
    print(f"Main: Failed to register GraphQL: {e}")

# --------------------------------------------------------------------------
# Webhook Test Endpoint
# --------------------------------------------------------------------------

@app.route("/api/webhooks/test", methods=["POST"])
@require_auth
def test_webhook():
    """
    Test a webhook URL with a mock payload.
    ---
    tags:
      - Webhooks
    parameters:
      - in: body
        name: body
        schema:
          type: object
        required: true
        description: Webhook config.
    responses:
      200:
        description: Webhook tested successfully.
    """
    body = request.get_json(force=True, silent=False) or {}
    url = body.get("url")
    secret = body.get("secret", "")
    tenant_id = body.get("tenantId", "test-tenant")
    
    if not url:
        return jsonify({"error": "url is required"}), 400
        
    mock_payload = {
        "event": "job.completed",
        "tenantId": tenant_id,
        "jobId": str(uuid.uuid4()),
        "status": "completed",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "summary": {
             "totalRows": 100,
             "created": 95,
             "errors": 5
        }
    }
    
    headers = {"Content-Type": "application/json"}
    if secret:
        # Include signature header if secret is provided
        import hmac, hashlib, json
        payload_bytes = json.dumps(mock_payload).encode('utf-8')
        signature = hmac.new(secret.encode('utf-8'), payload_bytes, hashlib.sha256).hexdigest()
        headers["X-Webhook-Signature"] = signature
        
    try:
        resp = requests.post(url, json=mock_payload, headers=headers, timeout=5)
        return jsonify({
            "status_code": resp.status_code,
            "response": resp.text[:500], # Limit response text
            "success": 200 <= resp.status_code < 300
        })
    except requests.exceptions.RequestException as e:
        return jsonify({
            "status_code": 0,
            "response": str(e),
            "success": False
        })

# --------------------------------------------------------------------------
# Audit Logs
# --------------------------------------------------------------------------

from audit_logger import log_audit_event

@app.route("/api/audit-logs", methods=["GET"])
@require_auth
@require_role(ROLE_ADMIN)
def get_audit_logs():
    """
    Get audit history for the tenant.
    ---
    tags:
      - Audit
    parameters:
      - in: query
        name: tenantId
        type: string
        required: true
    responses:
      200:
        description: List of audit events.
    """
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400
        
    limit = int(request.args.get("limit", 100))
    coll = get_audit_logs_collection()
    
    if coll is None:
        return jsonify({"logs": []})
        
    cursor = coll.find({"tenantId": tenant_id}).sort("timestamp", -1).limit(limit)
    logs = []
    
    # We could join users collection but for now just return the user_id that took the action
    
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        if "timestamp" in doc:
            doc["timestamp"] = doc["timestamp"].isoformat() + "Z"
        logs.append(doc)
        
    return jsonify({"logs": logs})

# --------------------------------------------------------------------------
# API Key Endpoints
# --------------------------------------------------------------------------

@app.route("/api/keys", methods=["GET"])
@require_auth
def get_api_keys():
    """
    List all active API keys for a tenant.
    ---
    tags:
      - API Keys
    parameters:
      - in: query
        name: tenantId
        type: string
        required: true
        description: The ID of the tenant.
    responses:
      200:
        description: A list of API keys for the tenant.
    """
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400
        
    keys = list_api_keys(tenant_id)
    return jsonify({"keys": keys})

@app.route("/api/keys", methods=["POST"])
@require_auth
def create_new_api_key():
    """
    Create a new API key for a tenant.
    ---
    tags:
      - API Keys
    parameters:
      - in: body
        name: body
        schema:
          type: object
        required: true
        description: Name for the new API key.
    responses:
      201:
        description: API Key created successfully. Returns the raw key once.
      400:
        description: Invalid request.
    """
    body = request.get_json(force=True, silent=False) or {}
    tenant_id = body.get("tenantId")
    name = body.get("name")
    
    if not tenant_id or not name:
        return jsonify({"error": "tenantId and name are required"}), 400
        
    user_id = get_current_user_id()
    doc, error = create_api_key(tenant_id, user_id, name)
    if error:
        return jsonify({"error": error}), 500
        
    return jsonify({"message": "API Key created", "key": doc}), 201

@app.route("/api/keys/<key_id>", methods=["DELETE"])
@require_auth
def delete_api_key(key_id):
    """
    Revoke an API key.
    ---
    tags:
      - API Keys
    parameters:
      - in: path
        name: key_id
        type: string
        required: true
        description: The ID of the API key to revoke.
      - in: query
        name: tenantId
        type: string
        required: true
        description: The ID of the tenant.
    responses:
      200:
        description: API key revoked successfully.
      404:
        description: Key not found or revoke failed.
    """
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400
        
    success = revoke_api_key(tenant_id, key_id)
    if success:
        return jsonify({"message": "API key revoked"})
    return jsonify({"error": "Failed to revoke key"}), 400

# --------------------------------------------------------------------------
# Template + field metadata (with PII flags + schema hints)
# --------------------------------------------------------------------------
from core_config import (
    PEOPLE_STATUSES, 
    BOOKING_STATUSES, 
    PATIENT_STATUSES, 
    TEMPLATES
)
from template_utils import (
    get_execution_order,
    validate_template_dependencies,
    get_templates,
    get_template
)





# --------------------------------------------------------------------------
# Helpers: normalisation, validation, header matching
# --------------------------------------------------------------------------
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalise_header_string(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def levenshtein_ratio(a: str, b: str) -> float:
    """
    Pure-Python normalised Levenshtein similarity in [0, 1].
    1.0 = identical, 0.0 = completely different.
    """
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    la, lb = len(a), len(b)
    # Build the DP matrix inline (O(la*lb) time, O(min) space)
    prev = list(range(lb + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * lb
        for j, cb in enumerate(b, 1):
            if ca == cb:
                curr[j] = prev[j - 1]
            else:
                curr[j] = 1 + min(prev[j], curr[j - 1], prev[j - 1])
        prev = curr
    distance = prev[lb]
    return 1.0 - distance / max(la, lb)


def fuzzy_enum_match(value: str, allowed: list, threshold: float = 0.75) -> str | None:
    """
    Find the closest allowed enum value using Levenshtein similarity.
    Returns the canonical value if similarity >= threshold, else None.
    """
    if not value or not allowed:
        return None
    vl = value.strip().lower()
    best_ratio = 0.0
    best_match = None
    for a in allowed:
        r = levenshtein_ratio(vl, a.lower())
        if r > best_ratio:
            best_ratio = r
            best_match = a
    return best_match if best_ratio >= threshold else None


# ---------------------------------------------------------------------------
# Jaro-Winkler — used exclusively for header matching (T4).
# Better than Levenshtein for short strings: prefix-biased so
# PatientID / PatentID scores higher than a symmetric edit distance would.
# ---------------------------------------------------------------------------

def jaro_winkler(s1: str, s2: str, p: float = 0.1) -> float:
    """
    Pure-Python Jaro-Winkler similarity in [0, 1].
    p = scaling factor for prefix bonus (standard = 0.1).
    """
    if s1 == s2:
        return 1.0
    l1, l2 = len(s1), len(s2)
    if l1 == 0 or l2 == 0:
        return 0.0

    match_dist = max(l1, l2) // 2 - 1
    if match_dist < 0:
        match_dist = 0

    s1_matches = [False] * l1
    s2_matches = [False] * l2
    matches = 0
    transpositions = 0

    for i in range(l1):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, l2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(l1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches / l1 + matches / l2 + (matches - transpositions / 2) / matches) / 3

    # Winkler prefix bonus (up to 4 chars)
    prefix = 0
    for i in range(min(4, l1, l2)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break

    return jaro + prefix * p * (1 - jaro)


# ---------------------------------------------------------------------------
# Global synonym graph — Tier 3 of the waterfall.
# Keys are normalised field identifiers; values are lists of normalised
# uploaded-header variants that should map to that concept.
# All values are already lowercase with non-alphanumerics stripped.
# ---------------------------------------------------------------------------

GLOBAL_SYNONYMS: dict[str, list[str]] = {
    # Person identification
    "id":           ["personid", "pid", "personno", "personnum", "no", "num", "number", "ref", "code"],
    "firstname":    ["fname", "givenname", "forename", "first", "firstnm", "given"],
    "surname":      ["lname", "lastname", "familyname", "last", "lastnm", "family", "secondname"],
    "fullname":     ["name", "fullnm", "displayname"],
    "dateofbirth":  ["dob", "birthdate", "bday", "birthday", "birthdt", "dateofbirth", "dobirthday"],
    "dob":          ["dateofbirth", "birthdate", "bday", "birthday"],
    "gender":       ["sex", "gend", "gndr"],
    "title":        ["salutation", "prefix", "honorific"],
    "email":        ["emailaddress", "mail", "emailid", "emailaddr", "eaddress"],
    "phone":        ["phonenumber", "mobile", "cell", "tel", "telephone", "mob", "mobilenum"],
    "type":         ["persontype", "emptype", "employeetype", "candidatetype", "workertype", "category"],
    # Patient / clinical
    "patientid":    ["candidateno", "candidatenum", "candidateid", "employeeid", "empid",
                     "memberid", "personno", "pid", "patno", "patientno", "workerid"],
    "assessmentdate": ["apptdate", "appointmentdate", "visitdate", "testdate", "examdate",
                       "scheduleddate", "bookingdate", "assessdt"],
    "assessmentname":  ["assessmenttype", "examtype", "testtype", "examname", "visittype",
                        "appointmenttype", "programname", "programtype"],
    "blockname":    ["block", "blocknum", "session", "cohort", "groupname"],
    "tests":        ["testlist", "examination", "examinations", "testnames", "testsconducted"],
    "status":       ["recordstatus", "currentstatus", "state", "processingstatus", "bookingstatus"],
    "notes":        ["comments", "remarks", "clinicalnotes", "comment", "note", "observations"],
    # Booking
    "bookingref":   ["bookingno", "bookingnum", "refno", "refnum", "appointmentref",
                     "appointmentno", "bookingid", "scheduleid"],
    "personnumber": ["personno", "personnum", "empno", "employeeno", "candidateno",
                     "candidatenum", "candidateid", "workerid", "individualno"],
    "locationname": ["location", "clinic", "site", "venue", "address", "clinicname", "facilityname"],
    "providername": ["provider", "doctor", "dr", "clinician", "specialist", "healthprovider"],
}


def is_valid_email(v: str) -> bool:
    if not v:
        return False
    return EMAIL_RE.match(v.strip()) is not None


def to_title_case_name(v: str) -> str:
    if not v:
        return ""
    v = re.sub(r"\d+", "", str(v)).strip()
    if not v:
        return ""
    parts = re.split(r"\s+", v)
    parts = [p[:1].upper() + p[1:].lower() if p else "" for p in parts]
    return " ".join(parts)


def parse_to_dd_mm_yyyy(raw: str):
    if not raw:
        return None
    s = str(raw).strip()
    formats = [
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d.%m.%Y",
        "%m-%d-%Y",
        "%m/%d/%Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%d-%m-%Y")
        except ValueError:
            continue
    return None


def clean_numeric_id(raw, required: bool):
    if raw is None:
        return "", ("bad" if required else "ok")
    s = str(raw)
    digits = re.sub(r"\D+", "", s)
    if digits:
        return digits, "ok"
    return "", ("bad" if required else "ok")


def clean_people_type(raw):
    if raw is None or str(raw).strip() == "":
        return "", "bad"

    v = str(raw).strip().lower()
    if v in ("candidate", "cand", "c"):
        return "Candidate", "ok"
    if v in ("employee", "emp", "e", "worker", "staff", "person"):
        return "Employee", "ok"
    return str(raw).strip(), "bad"


def clean_title(raw, required: bool):
    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")

    v = str(raw).strip().lower().replace(".", "")
    mapping = {
        "mr": "Mr",
        "ms": "Ms",
        "mrs": "Mrs",
        "miss": "Miss",
        "dr": "Dr",
        "doctor": "Dr",
    }
    if v in mapping:
        return mapping[v], "ok"
    return str(raw).strip(), ("bad" if required else "ok")


def clean_gender(raw, required: bool):
    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")

    v = str(raw).strip().lower()
    if v in ("m", "male"):
        return "M", "ok"
    if v in ("f", "female"):
        return "F", "ok"
    if v in ("other", "o", "non-binary", "nonbinary", "nb"):
        return "Other", "ok"
    return str(raw).strip(), ("bad" if required else "ok")



def is_valid_email(s):
    if not s: return False
    # Basic regex for email
    return re.match(r"[^@]+@[^@]+\.[^@]+", s)


def clean_email(raw, required: bool):
    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")

    v = str(raw).strip().replace(" ", "").lower()
    v = v.replace("(at)", "@").replace("[at]", "@")

    if "@" in v:
        local, dom = v.split("@", 1)
        if "." not in dom:
            dom = dom + ".com"
        v = f"{local}@{dom}"

    if is_valid_email(v):
        return v, "ok"
    return v, "bad"


def clean_booking_status(raw, required: bool):
    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")

    v = str(raw).strip().lower()
    if "pend" in v:
        return "Pending", "ok"
    if "book" in v or "bkd" in v or "confirm" in v:
        return "Booked", "ok"
    if "compl" in v or "done" in v or "fin" in v:
        return "Completed", "ok"
    if "cancel" in v or "cncl" in v:
        return "Cancelled", "ok"
    # Check if it matches any allowed status exactly (case-insensitive)
    if v.capitalize() in [s.lower().capitalize() for s in BOOKING_STATUSES]:
        return v.capitalize(), "ok"
    
    # If no pattern matched and not in allowed list, it's invalid
    return str(raw).strip(), ("bad" if required else "ok")


def clean_patient_status(raw, required: bool):
    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")

    v = str(raw).strip().lower()
    if "pend" in v or "await" in v:
        return "Pending", "ok"
    if "progress" in v or "ongo" in v:
        return "In Progress", "ok"
    if "compl" in v or "done" in v or "fin" in v:
        return "Completed", "ok"
    if "cancel" in v or "cncl" in v:
        return "Cancelled", "ok"
    # Check if it matches any allowed status exactly (case-insensitive)
    if v.capitalize() in [s.lower().capitalize() for s in PATIENT_STATUSES]:
        return v.capitalize(), "ok"
    
    # If no pattern matched and not in allowed list, it's invalid
    return str(raw).strip(), ("bad" if required else "ok")


TEST_SYNONYMS = {
    "audio": "Audiometry",
    "audiometry": "Audiometry",
    "spiro": "Spirometry",
    "spirometry": "Spirometry",
    "med": "Medical",
    "rtw": "Return to Work",
    "func": "Functional Assessment",
    "fce": "Functional Capacity Evaluation",
    "physio": "Physiotherapy",
}


def clean_tests_string(raw, required: bool):
    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")

    s = str(raw).strip()
    parts = re.split(r"[,+/&;-]+", s)
    cleaned_parts = []

    for p in parts:
        token = p.strip()
        if not token:
            continue
        key = re.sub(r"[^a-z0-9]", "", token.lower())
        canonical = TEST_SYNONYMS.get(key)
        cleaned_parts.append(canonical if canonical else to_title_case_name(token))

    if not cleaned_parts:
        return "", ("bad" if required else "ok")

    final = ", ".join(dict.fromkeys(cleaned_parts))  # stable de-dupe
    return final, "ok"


def clean_assessment_name(raw, required: bool):
    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")

    v = str(raw).strip()
    key = re.sub(r"[^a-z0-9]", "", v.lower())
    mapping = {
        "pem": "Pre-employment Medical",
        "preemploymentmedical": "Pre-employment Medical",
        "rtw": "Return to Work Assessment",
    }
    if key in mapping:
        return mapping[key], "ok"
    return to_title_case_name(v), "ok"


def clean_generic_string(raw, required: bool):
    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")
    return str(raw).strip(), "ok"


def clean_people_value(field, raw):
    key = field["key"]
    required = bool(field.get("required", False))

    if key == "type":
        return clean_people_type(raw)
    if key == "id":
        return clean_numeric_id(raw, required)
    if key == "title":
        return clean_title(raw, required)
    if key in ("firstName", "surname"):
        v = to_title_case_name(raw)
        if not v:
            return "", ("bad" if required else "ok")
        return v, "ok"
    if key == "gender":
        return clean_gender(raw, required)
    if key == "dob":
        s = str(raw).strip() if raw is not None else ""
        parsed = parse_to_dd_mm_yyyy(s)
        if not parsed:
            return "", "bad"
        return parsed, "ok"
    if key == "email":
        return clean_email(raw, required)

    return clean_generic_string(raw, required)


def clean_bookings_value(field, raw):
    key = field["key"]
    required = bool(field.get("required", False))

    if key == "bookingRef":
        if raw is None or str(raw).strip() == "":
            return "", "bad"
        return str(raw).strip(), "ok"
    if key == "personNumber":
        return clean_numeric_id(raw, required)
    if key == "assessmentName":
        return clean_assessment_name(raw, required)
    if key == "assessmentDate":
        s = str(raw).strip() if raw is not None else ""
        parsed = parse_to_dd_mm_yyyy(s)
        if not parsed:
            return "", "bad"
        return parsed, "ok"
    if key in ("locationName", "providerName"):
        return clean_generic_string(raw, required)
    if key == "status":
        return clean_booking_status(raw, required)

    return clean_generic_string(raw, required)


def clean_patient_value(field, raw):
    key = field["key"]
    required = bool(field.get("required", False))

    if key == "patientId":
        return clean_numeric_id(raw, required)
    if key == "assessmentName":
        return clean_assessment_name(raw, required)
    if key == "blockName":
        if raw is None or str(raw).strip() == "":
            return "", "ok"
        return to_title_case_name(raw), "ok"
    if key == "tests":
        return clean_tests_string(raw, required)
    if key == "status":
        return clean_patient_status(raw, required)
    if key == "notes":
        if raw is None or str(raw).strip() == "":
            return "", "ok"
        return str(raw).strip(), "ok"
    return clean_generic_string(raw, required)


def clean_value_by_pattern(field: dict, raw) -> tuple:
    """
    Generic pattern-driven cleaner for ANY template field.
    Uses field metadata (pattern, normalize, mask, allowed, etc.) so custom
    templates get the same quality of cleaning as system templates.
    Returns (clean_value, status) where status is 'ok' or 'bad'.
    """
    required = bool(field.get("required", False))
    pattern = field.get("pattern", "string")
    normalize = field.get("normalize", "")

    if raw is None or str(raw).strip() == "":
        return "", ("bad" if required else "ok")

    s = str(raw).strip()

    if pattern == "date":
        parsed = parse_to_dd_mm_yyyy(s)
        if not parsed:
            return s, "bad"
        return parsed, "ok"

    if pattern == "integer":
        return clean_numeric_id(raw, required)

    if pattern == "email":
        return clean_email(raw, required)

    if pattern == "enum":
        allowed = field.get("allowed") or []
        if allowed:
            # 1. Case-insensitive exact match
            match = next((a for a in allowed if a.lower() == s.lower()), None)
            if match:
                return match, "ok"
            # 2. Fuzzy nearest-enum repair
            fuzzy = fuzzy_enum_match(s, allowed, threshold=0.72)
            if fuzzy:
                return fuzzy, "ok"
            return s, ("bad" if not field.get("allowNew") else "ok")
        return s, "ok"

    # Default: string with optional normalisation
    if normalize == "titlecase":
        return to_title_case_name(s) or s, "ok"
    if normalize == "uppercase" or field.get("mask") == "uppercase":
        return s.upper(), "ok"
    if normalize == "email":
        return clean_email(raw, required)

    return s, "ok"


def _normalize_id(val) -> str:
    """Helper to convert any ID (numeric/string) to a consistent lowercase string.
    Strips non-digits to ensure IDs like 'e1005' match '1005' consistently.
    """
    if val is None:
        return ""
    s = str(val).strip().lower()
    digits = re.sub(r"\D+", "", s)
    if digits:
        return digits
    return s


def clean_value(template_key: str, field: dict, raw, valid_ref_ids: dict = None, session_context: list = None, all_templates: dict = None):
    """
    Main entry point for cleaning and validating a single value.
    Enforces pattern matching (date, integer, enum) and template-specific rules.
    """
    # 1. Handle Default Value (if raw is empty/None)
    if raw is None or str(raw).strip() == "":
        default_val = field.get("defaultValue")
        if default_val:
            raw = default_val

    # 2. Handle Explicit Trim Config
    if field.get("trim") and raw is not None:
         if isinstance(raw, str):
             raw = raw.strip()

    res = (None, None)
    
    # Legacy hardcoded cleaners FIRST (to preserve existing behavior for specific templates)
    if template_key == "People":
        res = clean_people_value(field, raw)
    elif template_key == "Bookings":
        res = clean_bookings_value(field, raw)
    elif template_key == "PatientData":
        res = clean_patient_value(field, raw)
    else:
        # Custom templates: use generic pattern-driven cleaner instead of bare string clean
        res = clean_value_by_pattern(field, raw)

    clean_val, status = res
    
    # If the legacy cleaner said "bad", we usually trust it. 
    # BUT if it said "ok", we must now run the AUTOMATED validations from the Builder.
    
    pattern = field.get("pattern")
    required = bool(field.get("required", False))
    s = str(clean_val).strip() if clean_val is not None else ""

    if not s:
        # If empty after cleaning
        if required:
            return "", "bad"
        return "", "ok"

    if status == "ok":
        # --- GENERAL TEXT VALIDATION ---
        if pattern == "string":
            # Min/Max Length
            min_len = int(field.get("minLength") or 0)
            max_len = int(field.get("maxLength") or 0)
            if min_len > 0 and len(s) < min_len:
                return clean_val, "bad" # Too short
            if max_len > 0 and len(s) > max_len:
                return clean_val, "bad" # Too long
            
            # Input Mask
            mask = field.get("mask")
            if mask == "alphanumeric":
                if not re.match(r"^[a-zA-Z0-9\s]*$", s): return clean_val, "bad"
            elif mask == "letters":
                if not re.match(r"^[a-zA-Z\s]*$", s): return clean_val, "bad"
            elif mask == "no_special":
                if not re.match(r"^[a-zA-Z0-9\s]*$", s): return clean_val, "bad"
            elif mask == "uppercase":
                clean_val = s.upper() # Auto-convert
                return clean_val, "ok"

        # --- NUMBER VALIDATION ---
        if pattern == "integer":
            # Decimals?
            allow_decimals = field.get("allowDecimals", False)
            allow_negative = field.get("allowNegative", False)

            try:
                num = float(s)
                if not allow_decimals and not s.isdigit() and int(num) != num:
                     return clean_val, "bad" # Float not allowed
                
                if not allow_negative and num < 0:
                     return clean_val, "bad" # Negative not allowed

                # Range
                min_val = field.get("minValue")
                max_val = field.get("maxValue")
                if min_val is not None and str(min_val).strip() != "":
                    if num < float(min_val): return clean_val, "bad"
                if max_val is not None and str(max_val).strip() != "":
                    if num > float(max_val): return clean_val, "bad"

            except ValueError:
                return clean_val, "bad" # Not a number

        # --- DATE VALIDATION ---
        if pattern == "date":
            # Strict date check: must be parseable and NOT just a number
            if s.isdigit() and len(s) < 8: # '123' etc are not dates
                return clean_val, "bad"
            
            parsed_date = parse_to_dd_mm_yyyy(s)
            if not parsed_date:
                return clean_val, "bad"
            
            # Range Logic
            date_range = field.get("dateRange")
            if date_range:
                try:
                    dt = datetime.strptime(parsed_date, "%d-%m-%Y")
                    now = datetime.now()
                    # Strip time for pure date comparison
                    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

                    if date_range == "past" and dt >= today:
                        return clean_val, "bad" # Must be past
                    if date_range == "future" and dt <= today:
                        return clean_val, "bad" # Must be future
                    if date_range == "recent_30":
                         delta = today - dt
                         if delta.days < 0 or delta.days > 30: return clean_val, "bad"
                    if date_range == "recent_365":
                         delta = today - dt
                         if delta.days < 0 or delta.days > 365: return clean_val, "bad"
                except:
                    pass # Date math failed, safe fallback
            
            return parsed_date, "ok"

        # --- EMAIL VALIDATION ---
        if pattern == "email":
            # Already structurally validated by clean_email, now store specific rules
            block_free = field.get("blockFreeEmail", False)
            if block_free:
                free_domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com", "aol.com"]
                if any(d in s.lower() for d in free_domains):
                    return clean_val, "bad"

            whitelist_str = field.get("domainWhitelist")
            if whitelist_str:
                allowed = [x.strip().lower() for x in whitelist_str.split(",") if x.strip()]
                domain = s.split("@")[-1].lower() if "@" in s else ""
                if domain not in allowed:
                    return clean_val, "bad"

        # --- ENUM VALIDATION (Case Sensitivity) ---
        if pattern == "enum":
            allowed = field.get("allowed") or []
            case_sensitive = field.get("caseSensitive", False)

            if not case_sensitive:
                # Check case-insensitive match
                match = next((a for a in allowed if a.lower() == s.lower()), None)
                if match:
                    return match, "ok" # Auto-correct casing

            if s not in allowed and not field.get("allowNew", False):
                return clean_val, "bad"

        # --- RELATIONSHIP VALIDATION (FK existence check) ---
        if pattern == "relationship":
            s_norm = _normalize_id(s) # Use the already cleaned 's'
            ref_set = (valid_ref_ids or {}).get(field["key"])
            
            if ref_set is not None:
                if s_norm in ref_set:
                    return s_norm, "ok"
                
                # FALLBACK: Aggressive search in session context if not in pre-fetched set
                if session_context:
                    rel = field.get("relationship") or {}
                    target_tpl_key = rel.get("targetTemplate")
                    if target_tpl_key:
                        for item in session_context:
                            if item.get("templateKey") == target_tpl_key:
                                t_rows = item.get("rows") or []
                                try:
                                    t_tpl = get_template(target_tpl_key, all_templates)
                                    id_f = next((f["key"] for f in t_tpl["fields"] if f.get("identifier")), "id")
                                    for r in t_rows:
                                        if _normalize_id(r.get(id_f)) == s_norm:
                                            print(f"CLEAN: Aggressive found {s_norm} in {target_tpl_key}")
                                            sys.stdout.flush()
                                            return s_norm, "ok"
                                except:
                                    continue
                
                print(f"CLEAN: {template_key}.{field['key']} FAILED to find ID '{s_norm}'")
                sys.stdout.flush()
                return clean_val, "bad"  # FK violation
            return s_norm, "ok" # If ref_set is None, skip check and assume ok

    return clean_val, status


def infer_sequential_numeric_ids(template_key: str, rows: list, all_templates: dict = None):
    """
    Fill single obvious numeric ID gaps algorithmically.
    Uses fields tagged identifier=True.

    Returns list of:
    { "rowIndex": int, "fieldKey": str, "newValue": str, "reason": str }
    """
    tpl = get_template(template_key, all_templates)
    id_fields = [f["key"] for f in tpl["fields"] if f.get("identifier")]
    inferred = []

    for fk in id_fields:
        seq = []
        for idx, row in enumerate(rows):
            v = row.get(fk)
            if v is None:
                continue
            s = str(v).strip()
            if s.isdigit():
                seq.append({"idx": idx, "val": int(s)})

        if len(seq) < 3:
            continue

        seq.sort(key=lambda x: x["idx"])

        # internal gaps
        for i in range(len(seq) - 1):
            a = seq[i]
            b = seq[i + 1]
            if b["val"] - a["val"] == 2 and b["idx"] - a["idx"] >= 2:
                mid_idx = a["idx"] + 1
                if mid_idx < len(rows):
                    mid_val = rows[mid_idx].get(fk)
                    if (
                        not mid_val
                        or str(mid_val).strip() == ""
                        or str(mid_val).strip().lower() == "unknown"
                    ):
                        new_val = str(a["val"] + 1)
                        rows[mid_idx][fk] = new_val
                        row_index_label = rows[mid_idx].get("__rowIndex", mid_idx)
                        inferred.append(
                            {
                                "rowIndex": int(row_index_label),
                                "fieldKey": fk,
                                "newValue": new_val,
                                "reason": "internal-sequence-gap",
                            }
                        )

        # trailing gap
        last_row_idx = len(rows) - 1
        if last_row_idx < 0:
            continue

        last_cell = rows[last_row_idx].get(fk)
        if last_cell and str(last_cell).strip().lower() not in ("", "unknown"):
            continue

        steps = []
        for i in range(len(seq) - 1):
            dv = seq[i + 1]["val"] - seq[i]["val"]
            if dv > 0:
                steps.append(dv)

        if len(steps) >= 2 and all(s == 1 for s in steps) and seq[-1]["idx"] < last_row_idx:
            new_val = str(seq[-1]["val"] + 1)
            rows[last_row_idx][fk] = new_val
            row_index_label = rows[last_row_idx].get("__rowIndex", last_row_idx)
            inferred.append(
                {
                    "rowIndex": int(row_index_label),
                    "fieldKey": fk,
                    "newValue": new_val,
                    "reason": "trailing-sequence",
                }
            )

    return inferred


def get_valid_ref_ids(template_key: str, all_templates: dict, tenant_id: str = None, session_context: list = None):
    """
    Collect all valid target IDs for relationship fields from both DB and session context.
    Normalized strings are used for comparison.
    """
    tpl = get_template(template_key, all_templates)
    refs = tpl.get("references", {})
    valid_ref_ids = {}

    if not refs:
        return {}

    # 1. Collect IDs from session_context (in-memory)
    session_ids_by_template = {}
    if session_context:
        for item in session_context:
            t_key = item.get("templateKey")
            t_rows = item.get("rows") or []
            if t_key and t_rows:
                try:
                    t_tpl = get_template(t_key, all_templates)
                    # Use provided identifier or fall back to 'id'
                    id_field = next((f["key"] for f in t_tpl["fields"] if f.get("identifier")), "id")
                    
                    # Log for debug
                    try:
                        log_path = r"C:\Users\abrin\Desktop\debug_validation.txt"
                        with open(log_path, "a") as f_log:
                            f_log.write(f"CONTEXT: {t_key} rows={len(t_rows)} id_field={id_field}\n")
                            found_ids = [_normalize_id(r.get(id_field)) for r in t_rows if r.get(id_field) is not None]
                            if "1005" in found_ids:
                                f_log.write(f"FOUND 1005 in {t_key} context!\n")
                            else:
                                f_log.write(f"1005 MISSING from {t_key} context. First 5 IDs: {found_ids[:5]}\n")
                    except:
                        pass

                    session_ids_by_template[t_key] = {
                        _normalize_id(r.get(id_field)) 
                        for r in t_rows 
                        if r.get(id_field) is not None
                    }
                except Exception as e:
                    print(f"CLEAN: Error processing session context for {t_key}: {e}")
                    continue

    # 2. Collect IDs from DB
    if tenant_id:
        try:
            from db_config import get_tenant_data_collection
            coll = get_tenant_data_collection()
            if coll is not None:
                for field_key, ref_config in refs.items():
                    target_tpl = ref_config.get("targetTemplate")
                    target_field = ref_config.get("targetField", "id")
                    if not target_tpl:
                        continue
                    
                    # Fetch DB IDs
                    docs = coll.find(
                        {"tenantId": tenant_id, "templateKey": target_tpl},
                        {f"data.{target_field}": 1}
                    )
                    ids = {
                        _normalize_id(d["data"][target_field])
                        for d in docs
                        if "data" in d and target_field in d["data"]
                    }
                    
                    # Merge with session IDs
                    if target_tpl in session_ids_by_template:
                        ids.update(session_ids_by_template[target_tpl])
                        print(f"CLEAN: Merged {len(session_ids_by_template[target_tpl])} session IDs for '{target_tpl}'")

                    valid_ref_ids[field_key] = ids
        except Exception as e:
            print(f"CLEAN: Error pre-fetching ref IDs: {e}")

    return valid_ref_ids


def clean_rows_for_template(template_key: str, rows: list, all_templates: dict = None, tenant_id: str = None, session_context: list = None, valid_ref_ids: dict = None):
    """
    Algorithmically clean + validate all rows for a given template.
    """
    tpl = get_template(template_key, all_templates)
    
    # If not provided, fetch them (legacy support or first call)
    if valid_ref_ids is None:
        valid_ref_ids = get_valid_ref_ids(template_key, all_templates, tenant_id, session_context)

    cleaned_rows = []
    row_errors = {}

    for row in rows:
        idx = row.get("__rowIndex")
        if idx is None:
            idx = len(cleaned_rows)
        idx = int(idx)

        cleaned = {"__rowIndex": idx}
        errors_for_row = []

        for field in tpl["fields"]:
            fkey = field["key"]
            raw_val = row.get(fkey)

            clean_val, status = clean_value(template_key, field, raw_val, valid_ref_ids=valid_ref_ids, session_context=session_context, all_templates=all_templates)

            # PatientData.blockName default
            if template_key == "PatientData" and fkey == "blockName" and not clean_val:
                clean_val = "Block1"

            cleaned[fkey] = clean_val

            if status != "ok":
                errors_for_row.append(fkey)

        if errors_for_row:
            row_errors[idx] = errors_for_row

        cleaned_rows.append(cleaned)

    inferred_ids = infer_sequential_numeric_ids(template_key, cleaned_rows, all_templates)
    return cleaned_rows, row_errors, inferred_ids


# --------------------------------------------------------------------------
# Header mapping helpers
# --------------------------------------------------------------------------
# -- date/email/int sample-value detectors used by header scoring --
_DATE_RE = re.compile(
    r"^\d{1,2}[\-/\.]\d{1,2}[\-/\.]\d{2,4}$|^\d{4}[\-/\.]\d{1,2}[\-/\.]\d{1,2}$"
)
_EMAIL_RE2 = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_INT_RE = re.compile(r"^-?\d+$")


def _sample_value_bonus(field: dict, sample_values: list) -> float:
    """
    Extra score based on whether the actual data values in a CSV column
    look like they belong to this field.
    """
    if not sample_values:
        return 0.0
    non_empty = [str(v).strip() for v in sample_values if v is not None and str(v).strip()]
    if not non_empty:
        return 0.0

    pattern = field.get("pattern", "")
    allowed = field.get("allowed") or []
    bonus = 0.0
    sample_n = len(non_empty)

    if pattern == "date":
        hits = sum(1 for v in non_empty if _DATE_RE.match(v))
        bonus += 8.0 * (hits / sample_n)

    elif pattern == "email":
        hits = sum(1 for v in non_empty if _EMAIL_RE2.match(v))
        bonus += 8.0 * (hits / sample_n)

    elif pattern == "integer":
        hits = sum(1 for v in non_empty if _INT_RE.match(v))
        bonus += 5.0 * (hits / sample_n)

    elif pattern == "enum" and allowed:
        allowed_lower = {a.lower() for a in allowed}
        hits = sum(
            1 for v in non_empty
            if v.lower() in allowed_lower or fuzzy_enum_match(v, allowed, threshold=0.8) is not None
        )
        bonus += 7.0 * (hits / sample_n)

    return bonus


def compute_header_score(field: dict, header: str, all_templates: dict = None,
                         sample_values: list = None) -> float:
    """Heuristic score for mapping a given uploaded header to a template field.

    Signals (highest first):
      +10  exact label match
       +9  exact synonym match
       +8  exact key match
       +8  sample-value → enum hit rate
       +8  sample-value → date/email hit rate
       +6  fuzzy label/key match (Levenshtein ≥ 0.80)
       +5  substring label in header (or vice-versa)
       +5  sample-value → integer hit rate
       +4  synonym substring overlap
      +1.5 template keyword in header
    """
    if not header:
        return 0.0

    hl = header.lower()
    fl = field["label"].lower()
    fk = field["key"].lower()
    normh = normalise_header_string(header)
    normfl = normalise_header_string(field["label"])
    normfk = normalise_header_string(field["key"])

    score = 0.0

    # --- Exact matches ---
    if hl == fl or normh == normfl:
        score += 10.0
    if hl == fk or normh == normfk:
        score += 8.0
    if fl in hl or hl in fl:
        score += 5.0

    # --- Synonym matches ---
    for syn in field.get("synonyms", []) or []:
        sl = syn.lower()
        norms = normalise_header_string(syn)
        if hl == sl or normh == norms:
            score += 9.0
            break  # already very confident, no need to keep adding
        elif sl in hl or hl in sl:
            score += 4.0

    # --- Fuzzy distance (catches typos / CamelCase confusion) ---
    if score < 8.0:  # only needed when exact match hasn't already won
        best_fuzzy = max(
            levenshtein_ratio(normh, normfl),
            levenshtein_ratio(normh, normfk),
        )
        for syn in field.get("synonyms", []) or []:
            best_fuzzy = max(best_fuzzy, levenshtein_ratio(normh, normalise_header_string(syn)))
        if best_fuzzy >= 0.80:
            score += 6.0 * best_fuzzy  # e.g. 0.85 → +5.1, 1.0 → +6.0

    # --- Sample-value inference ---
    score += _sample_value_bonus(field, sample_values or [])

    # --- Template keyword boost ---
    try:
        kw_list = get_template(field["templateKey"], all_templates)["keywords"]
        for kw in kw_list:
            if kw in hl:
                score += 1.5
    except (ValueError, KeyError):
        pass

    return score


def suggest_header_mappings(template_key: str, uploaded_headers: list, current_mapping: dict,
                            all_templates: dict = None, sample_rows: list = None):
    """Thin wrapper — delegates to the waterfall pipeline."""
    return waterfall_header_match(
        template_key=template_key,
        uploaded_headers=uploaded_headers,
        tenant_id=None,
        current_mapping=current_mapping,
        all_templates=all_templates,
        sample_rows=sample_rows,
    )


def waterfall_header_match(
    template_key: str,
    uploaded_headers: list,
    tenant_id: str,
    current_mapping: dict = None,
    all_templates: dict = None,
    sample_rows: list = None,
) -> list:
    """
    4-Tier Waterfall Header Matching Pipeline.

    Each field passes through tiers in order and stops at the first match.
    Tiers:
      T1  Normalisation smash — strip all non-alphanumeric, lowercase, exact ==
      T2  Tenant memory      — recall previously confirmed mappings from DB
      T3  Global synonym graph — hardcoded B2B synonym dict
      T4  Jaro-Winkler fuzzy  — typo tolerance, threshold 0.88

    Returns list of {templateKey, matchedHeader, confidence, source}.
    """
    tpl = get_template(template_key, all_templates)
    current_mapping = current_mapping or {}
    used_headers: set[str] = set(h for h in current_mapping.values() if h)
    mappings = []

    # --- Pre-compute normalised header strings once ---
    norm_uploaded: dict[str, str] = {h: normalise_header_string(h) for h in uploaded_headers}
    # Reverse map: normalised -> original (for T1/T3 lookups)
    norm_to_orig: dict[str, str] = {v: k for k, v in norm_uploaded.items()}

    # --- T2: Load tenant aliases from DB once for this template ---
    alias_lookup: dict[str, str] = {}   # fieldKey -> uploadedHeader (if known alias)
    if tenant_id:
        try:
            coll = get_header_aliases_collection()
            if coll is not None:
                docs = coll.find({"tenantId": tenant_id, "templateKey": template_key})
                for doc in docs:
                    fk = doc.get("fieldKey")
                    uh = doc.get("uploadedHeader")
                    # Only useful if the header is present in this upload
                    if fk and uh and uh in norm_uploaded:
                        alias_lookup[fk] = uh
        except Exception as e:
            print(f"WATERFALL: T2 alias load error: {e}")

    # --- T3: Build per-field synonym set from GLOBAL_SYNONYMS ---
    # Allows both directions: field norm matches synonym, or synonym matches field norm
    def _global_synonyms_for(field_norm: str) -> set[str]:
        result = set()
        # Direct lookup
        for v in GLOBAL_SYNONYMS.get(field_norm, []):
            result.add(v)
        # Reverse: is this field norm a synonym of some other concept?
        for concept, variants in GLOBAL_SYNONYMS.items():
            if field_norm in variants:
                result.add(concept)
                result.update(variants)
        return result

    # --- Per-field waterfall ---
    for field in tpl["fields"]:
        fkey = field["key"]
        fnorm = normalise_header_string(field.get("label", fkey))
        fknorm = normalise_header_string(fkey)
        field_synonyms_norm: set[str] = _global_synonyms_for(fnorm) | _global_synonyms_for(fknorm)
        # Also add field-level synonyms defined in the template
        for syn in (field.get("synonyms") or []):
            field_synonyms_norm.add(normalise_header_string(syn))

        base = {"templateKey": fkey, "matchedHeader": None, "confidence": 0.0, "source": "none"}

        # Honour already-confirmed mapping
        existing = current_mapping.get(fkey)
        if existing and existing in norm_uploaded:
            base["matchedHeader"] = existing
            base["confidence"] = 1.0
            base["source"] = "existing"
            mappings.append(base)
            used_headers.add(existing)
            continue

        matched_header = None
        confidence = 0.0
        source = "none"

        # ---- TIER 1: Normalisation smash ----
        # Check if any uploaded header, after stripping formatting, equals the field label or key
        for h, hn in norm_uploaded.items():
            if h in used_headers:
                continue
            if hn == fnorm or hn == fknorm:
                matched_header = h
                confidence = 1.0
                source = "T1-norm"
                break

        # ---- TIER 2: Tenant memory ----
        if not matched_header:
            alias_h = alias_lookup.get(fkey)
            if alias_h and alias_h not in used_headers:
                matched_header = alias_h
                confidence = 0.95
                source = "T2-memory"

        # ---- TIER 3: Global synonym graph ----
        if not matched_header:
            for h, hn in norm_uploaded.items():
                if h in used_headers:
                    continue
                if hn in field_synonyms_norm:
                    matched_header = h
                    confidence = 0.85
                    source = "T3-synonym"
                    break

        # ---- TIER 4: Jaro-Winkler fuzzy match ----
        JW_THRESHOLD = 0.88
        if not matched_header:
            best_jw = 0.0
            best_h = None
            for h, hn in norm_uploaded.items():
                if h in used_headers:
                    continue
                # T4 scores ONLY against the field's own label/key and template-defined synonyms.
                # Global synonyms (field_synonyms_norm) are intentionally excluded here because
                # T3 already handles exact synonym matches, and fuzzy-matching against a large
                # synonym set causes prefix-coincidence false-positives
                # (e.g. 'candidateid' ~ 'candidatetype' → wrongly matching Type field).
                template_syn_norms = [normalise_header_string(s) for s in (field.get("synonyms") or [])]
                jw = max(
                    jaro_winkler(hn, fnorm),
                    jaro_winkler(hn, fknorm),
                    *(jaro_winkler(hn, s) for s in template_syn_norms) if template_syn_norms else (0.0,)
                )
                if jw > best_jw:
                    best_jw = jw
                    best_h = h
            if best_h and best_jw >= JW_THRESHOLD:
                matched_header = best_h
                confidence = round(best_jw, 3)
                source = "T4-fuzzy"

        # ---- Fallback: sample-value scoring (last resort, non-waterfall) ----
        if not matched_header and sample_rows:
            header_samples = {}
            for h in uploaded_headers:
                vals = [r.get(h) for r in sample_rows[:10] if r.get(h) is not None]
                if vals:
                    header_samples[h] = vals

            best_score = 0.0
            best_h = None
            enriched = {**field, "templateKey": template_key}
            for h in uploaded_headers:
                if h in used_headers:
                    continue
                score = _sample_value_bonus(enriched, header_samples.get(h, []))
                if score > best_score:
                    best_score = score
                    best_h = h
            if best_h and best_score >= 5.0:
                matched_header = best_h
                confidence = round(min(best_score / 10.0, 0.75), 3)
                source = "T5-sample"

        if matched_header:
            print(f"WATERFALL: {source}: '{fkey}' → '{matched_header}' (conf={confidence})")
            used_headers.add(matched_header)
        else:
            print(f"WATERFALL: no-match: '{fkey}' left unmapped")

        base["matchedHeader"] = matched_header
        base["confidence"] = confidence
        base["source"] = source
        mappings.append(base)

    return mappings


def detect_template_from_headers(headers: list, all_templates: dict = None,
                                  sample_rows: list = None) -> str:
    """Heuristic to pick the most likely template based on headers + sample values."""
    detected_key = None
    best_score = 0.0

    source = all_templates if all_templates is not None else TEMPLATES

    # Build per-header samples once
    header_samples: dict[str, list] = {}
    if sample_rows:
        for h in headers:
            vals = [r.get(h) for r in sample_rows[:5] if r.get(h) is not None]
            if vals:
                header_samples[h] = vals

    for key, tpl in source.items():
        score = 0.0
        keywords = tpl.get("keywords") or []
        fields = tpl.get("fields") or []

        # 1. Keyword hit in header names
        for kw in keywords:
            for h in headers:
                if kw in (h or "").lower():
                    score += 1.0

        # 2. Sample-value field type matches
        for h in headers:
            samples = header_samples.get(h, [])
            if not samples:
                continue
            # Find best-scoring field in this template for this header
            for field in fields:
                enriched = {**field, "templateKey": key}
                score += _sample_value_bonus(enriched, samples) * 0.3  # weighted contribution

        if score > best_score:
            best_score = score
            detected_key = key

    return detected_key or "People"


def parse_csv_content(raw: str):
    """Parse raw CSV text into headers + data rows. Returns (headers, rows)."""
    if not raw:
        return [], []

    lines = [l for l in raw.splitlines() if l.strip()]
    if not lines:
        return [], []

    reader = csv.reader(lines)
    try:
        headers = next(reader)
    except StopIteration:
        return [], []

    headers = [h.strip() for h in headers]
    data_rows = [row for row in reader]
    return headers, data_rows


def build_template_rows_from_csv(template_key: str, headers: list, data_rows: list, header_mapping: dict, all_templates: dict = None):
    """Turn raw CSV rows into template-shaped rows using header_mapping."""
    tpl = get_template(template_key, all_templates)
    header_index = {h: i for i, h in enumerate(headers)}

    rows = []
    for idx, raw_row in enumerate(data_rows):
        row_obj = {"__rowIndex": idx}
        for field in tpl["fields"]:
            fkey = field["key"]
            mapped_header = header_mapping.get(fkey)
            val = ""
            if mapped_header and mapped_header in header_index:
                col_idx = header_index[mapped_header]
                if col_idx < len(raw_row):
                    val = raw_row[col_idx]
            row_obj[fkey] = val
        rows.append(row_obj)

    return rows


# --------------------------------------------------------------------------
# GPT integration
# --------------------------------------------------------------------------
def build_header_prompt(
    template_key: str, 
    uploaded_headers: list, 
    current_mapping: dict,
    template_fields: list = None,
    template_label: str = None,
    template_keywords: list = None,
    sample_data: list = None,
    all_templates: dict = None
) -> str:
    """Build a JSON prompt for GPT to map uploaded headers to template fields."""
    tpl = get_template(template_key, all_templates)

    # Use enriched templateFields if provided, otherwise build from template
    if template_fields:
        fields_payload = []
        for f in template_fields:
            field_obj = {
                "key": f.get("key", ""),
                "label": f.get("label", ""),
                "required": bool(f.get("required", False)),
                "isPii": bool(f.get("isPii", False)),
                "description": f.get("description", ""),
            }
            # Include allowedValues (enums) if available - crucial for intelligent matching
            if f.get("allowedValues"):
                field_obj["allowedValues"] = f.get("allowedValues")
            fields_payload.append(field_obj)
    else:
        # Fallback to building from template
        fields_payload = []
        for f in tpl["fields"]:
            field_obj = {
                "key": f["key"],
                "label": f.get("label", ""),
                "required": bool(f.get("required", False)),
                "identifier": bool(f.get("identifier", False)),
                "synonyms": f.get("synonyms") or [],
            }
            # Include allowed values from template
            if f.get("allowed"):
                field_obj["allowedValues"] = f.get("allowed")
            fields_payload.append(field_obj)

    current_list = []
    for k, v in (current_mapping or {}).items():
        current_list.append({"templateKey": k, "matchedHeader": v or None})

    # Enhanced instructions that leverage enum values and sample data
    rules = [
        'Return ONLY a JSON object of the form { "mappings": [ { "templateKey": string, "matchedHeader": string|null, "confidence": number } ] }.',
        "The response must be valid JSON with no extra text, comments, or code fences.",
        "Include exactly one mapping object for every field in template.fields.",
        "Never invent new header names. matchedHeader MUST be either null or one of uploadedHeaders.",
        "If no suitable header exists, set matchedHeader to null and confidence to 0.",
        "Prefer exact or near-exact matches (ignoring case, spaces, underscores, and camelCase).",
        "When no exact match exists, choose the most semantically similar header using meaning, common abbreviations, and token overlap.",
        "Use template.fields.label, .key, .description and .identifier to understand each field's purpose.",
        "Do not reuse the same uploaded header for multiple template fields unless it clearly represents a shared concept.",
        "For identifier-like fields (identifier=true OR label/key contains tokens such as 'id', 'no', 'number', 'code', 'ref'), treat uploaded headers containing 'id', 'no', 'number', 'code' or 'ref' as strong candidates.",
        "Treat common abbreviations and variants as equivalent, for example: 'emp' ≈ 'employee', 'cand' ≈ 'candidate', 'pers' ≈ 'person', 'pt' ≈ 'patient'.",
    ]
    
    # Add rules for enum-based matching
    rules.append(
        "CRITICAL: When a template field has allowedValues (enum), examine the sampleData to see which uploaded header column contains values matching those enums. "
        "For example, if a field has allowedValues=['Mr','Ms','Mrs','Miss','Dr'] and sampleData shows a column with values like 'Mr', 'Ms', 'Mrs', that column is almost certainly the correct match."
    )
    
    rules.append(
        "Use sampleData to infer field types: if sampleData shows email addresses (containing '@'), that column likely maps to an email field. "
        "If sampleData shows dates (DD-MM-YYYY, YYYY-MM-DD, etc.), that column likely maps to a date field. "
        "If sampleData shows names (like 'John', 'Mary', 'Smith'), those columns likely map to firstName/surname fields."
    )
    
    rules.append(
        "When templateKeywords are provided, use them to understand the domain context. Headers matching these keywords are more likely to be relevant."
    )

    prompt_obj = {
        "instructions": {
            "goal": "Map uploaded CSV headers to expected template fields using header names, enum values, and sample data patterns.",
            "rules": rules,
        },
        "template": {
            "name": tpl["key"],
            "label": template_label or tpl.get("label", ""),
            "keywords": template_keywords or tpl.get("keywords", []),
            "fields": fields_payload
        },
        "uploadedHeaders": uploaded_headers,
        "currentMapping": current_list,
    }
    
    # Include sample data if provided - this is crucial for pattern inference
    if sample_data:
        prompt_obj["sampleData"] = sample_data
        prompt_obj["instructions"]["rules"].append(
            "Analyze sampleData to understand the actual content patterns in each uploaded column. "
            "Match columns based on both header names AND the actual data values they contain."
        )

    return json.dumps(prompt_obj, indent=2)


def estimate_cost_from_usage(usage: dict) -> float:
    if not usage:
        return 0.0
    total_tokens = usage.get("total_tokens") or (
        (usage.get("prompt_tokens") or 0) + (usage.get("completion_tokens") or 0)
    )
    return total_tokens * 0.000002  # placeholder


def _strip_code_fences(s: str) -> str:
    """
    Remove ``` or ```json fenced blocks if the model returns them anyway.
    """
    if not s:
        return ""
    s = s.strip()

    if s.startswith("```"):
        # remove opening fence (``` or ```json etc.)
        s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
        # remove trailing fence
        s = re.sub(r"\s*```$", "", s)

    return s.strip()


def call_openai_chat(prompt: str, call_type: str, tenant_id: str = None, job_id: str = None, response_format: dict = None):
    """
    Call OpenAI ChatCompletion with the specified JSON prompt string.
    Returns (parsed_json, usage_dict) or (None, None) on error.
    """
    if not OPENAI_API_KEY:
        print("DEBUG: call_openai_chat: No OPENAI_API_KEY set.")
        return None, None
    
    print(f"DEBUG: call_openai_chat: Sending request to OpenAI... (Key length: {len(OPENAI_API_KEY)})")

    try:
        payload = {
            "model": OPENAI_MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a strict CSV import assistant. "
                        "You may be asked to clean rows or map headers, but you must always "
                        "follow the JSON output format described in the user prompt. "
                        "You only output raw JSON. No explanations or code fences."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0,
            "max_tokens": 2048,
        }

        if response_format:
            payload["response_format"] = response_format

        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=120,
        )
        if resp.status_code != 200:
            print(f"DEBUG: OpenAI API Error: {resp.status_code} - {resp.text}")
            return None, None

        resp_json = resp.json()
        content = resp_json["choices"][0]["message"]["content"]
        # usage
        usage = resp_json.get("usage", {})

        # Attempt to parse JSON from content
        # It might be wrapped in ```json ... ```
        cleaned = content.strip()
        if cleaned.startswith("```"):
            import re
            match = re.search(r"```(?:json)?(.*?)```", cleaned, re.DOTALL)
            if match:
                cleaned = match.group(1).strip()
        
        try:
            parsed = json.loads(cleaned)
            return parsed, usage
        except json.JSONDecodeError as e:
            print(f"DEBUG: JSON Parsing Failed: {e}")
            print(f"DEBUG: Failed content: {cleaned}")
            return None, usage

    except Exception as ex:
        print(f"DEBUG: call_openai_chat Exception: {ex}")
        return None, None


def get_json_schema_for_cleaning(template_key: str, tpl: dict, include_pii: bool):
    """
    Generate an OpenAI-compatible JSON schema for the 'clean' operation.
    """
    properties = {
        "__rowIndex": {"type": "integer"}
    }
    required = ["__rowIndex"]
    
    for f in tpl.get("fields", []):
        if f.get("is_pii") and not include_pii:
            continue
            
        fk = f["key"]
        f_schema = {"type": "string"} # Default to string
        
        # If there are allowed values, use enum
        if f.get("allowed"):
            f_schema["enum"] = f["allowed"]
            
        properties[fk] = f_schema
        # Note: We don't mark all fields as required here to allow model some flexibility 
        # unless ensureNoEmptyValues is set (which is handled by logic, not schema usually)
        # However, for Structured Output, the schema must be strict.
        required.append(fk)

    schema = {
        "type": "json_schema",
        "json_schema": {
            "name": "clean_csv_rows",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "outputRows": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": properties,
                            "required": required,
                            "additionalProperties": False
                        }
                    }
                },
                "required": ["outputRows"],
                "additionalProperties": False
            }
        }
    }
    return schema


# --------------------------------------------------------------------------
# (Everything below here is unchanged from your logic; just formatted.)

def build_clean_prompt(
    template_key: str,
    tpl: dict,
    rows: list,
    row_meta: list,
    settings: dict = None,
    full_dataset_context: list = None,
    extras: dict = None,
    include_pii_in_ai: bool = True,
) -> str:
    """
    Streamlined prompt building for token efficiency and caching.
    """
    settings = settings or {}
    extras = extras or {}

    # Dynamic Rules based on Settings
    ensure_no_empty = settings.get("ensureNoEmptyValues", False)
    
    pii_rule = "PII: If PII fields are missing or hidden, do not invent them unless 'ensureNoEmptyValues' is true."
    strictness_rule = "Strictness: Only hallucinate synthetic values if settings.ensureNoEmptyValues is true. Otherwise, leave uncertain fields blank."

    if ensure_no_empty:
        pii_rule = "PII & IDs: 'Ensure no empty values' is ENABLED. You MUST fill ALL missing IDs, PII, and required fields with plausible synthetic data (e.g., sequential numbers like '9901', '9902' or placeholder names). Do NOT leave any field empty."
        strictness_rule = "Strictness: You are REQUIRED to invent/hallucinate values for any empty fields to ensure the row is complete."

    instructions = {
        "goal": "Clean and normalise CSV rows according to the template schema while repairing errors and inferring missing data.",
        "rules": [
            "Maintain raw JSON output conforming exactly to the structured schema.",
            "Fix common errors: typos, casing, spacing, and formatting (Title Case for names, DD-MM-YYYY for dates).",
            "Repair sequential IDs: If a gap of one exists in a +1 sequence, fill it.",
            "Infer from context: Use email to fix names, gender to fix titles, or common domain patterns to fix broken emails.",
            "Terminology: Use professional medical/admin terms and expand obvious abbreviations (e.g., 'rtw' -> 'Return to Work').",
            pii_rule,
            strictness_rule
        ]
    }
    
    # Add Strict Selective Cleaning Instruction if enabled
    if settings.get("onlyFixInvalid"):
        selective_rule = "CRITICAL: The 'Clean Valid Values' flag is DISABLED. For each row, you MUST ONLY modify fields where 'isValid' is false in 'input.meta'. If 'isValid' is true for a field (even if it contains '???', 'Unknown', or placeholders), you MUST return the original value from 'input.rows' EXACTLY as provided."
        instructions["rules"].insert(0, selective_rule)
        instructions["goal"] = "Repair ONLY the reported errors in the CSV rows while leaving valid fields completely unchanged."

    # Build template schema compactly
    template_schema = []
    pii_field_keys = set()
    for f in tpl["fields"]:
        is_pii = bool(f.get("is_pii", False))
        if is_pii:
            pii_field_keys.add(f["key"])
        
        if is_pii and not include_pii_in_ai:
            continue
            
        template_schema.append({
            "key": f["key"],
            "label": f["label"],
            "required": bool(f.get("required", False)),
            "allowed": f.get("allowed") or None,
            "pattern": f.get("pattern"),
            "dateFormat": f.get("dateFormat")
        })

    # Filter rows to send only minimal data
    def filter_pii(data_list):
        if not data_list:
            return None
        if include_pii_in_ai or not pii_field_keys:
            return data_list
        filtered = []
        for item in data_list:
            f_item = {"__rowIndex": item.get("__rowIndex", 0)}
            for k, v in item.items():
                if k not in pii_field_keys:
                    f_item[k] = v
            filtered.append(f_item)
        return filtered

    payload = {
        "instructions": instructions,
        "template": {"key": template_key, "schema": template_schema},
        "context": {
            "settings": settings,
            "fieldSamples": extras.get("fieldSamples") if extras else None,
            "fullDatasetContext": filter_pii(full_dataset_context)
        },
        "input": {
            "rows": filter_pii(rows),
            "meta": filter_pii(row_meta)
        }
    }

    return json.dumps(payload, separators=(',', ':'))


def build_row_meta_for_prompt(template_key: str, tpl: dict, cleaned_rows: list, row_errors: dict):
    """Build inputRowMeta shape for AI prompt from validation errors."""
    meta = []
    for r in cleaned_rows:
        idx = int(r.get("__rowIndex", 0))
        errors_for_row = set(row_errors.get(idx, []))

        fields_meta = {}
        for f in tpl["fields"]:
            fk = f["key"]
            fields_meta[fk] = {"isValid": fk not in errors_for_row, "isRequired": bool(f.get("required", False))}

        meta.append({"__rowIndex": idx, "fields": fields_meta})

    return meta


def enhance_rows_with_ai(
    template_key: str,
    tpl: dict,
    cleaned_rows: list,
    row_errors: dict,
    tenant_id: str,
    job_id: str,
    use_ai: bool,
    settings: dict = None,
    full_dataset_context: list = None,
    extras: dict = None,
    include_pii_in_ai: bool = True,
    valid_ref_ids: dict = None,
):
    """
    Optional GPT pass using schema-driven prompt.
    Returns (possibly updated rows, ai_usage_summary or None).
    """
    MAX_AI_ROWS = 300
    
    # --- Selective Cleaning (Efficiency Optimization) ---
    only_fix_invalid = settings.get("onlyFixInvalid", False) if settings else False
    if only_fix_invalid:
        # Note: row_errors keys can be int or str depending on source
        error_indices = set()
        for k in row_errors.keys():
            try:
                error_indices.add(int(k))
            except:
                error_indices.add(k)
        
        initial_list = cleaned_rows[:]
        rows_to_process_pre = [r for r in initial_list if r.get("__rowIndex") in error_indices]
        skipped_rows_pre = [r for r in initial_list if r.get("__rowIndex") not in error_indices]
        
        print(f"DEBUG: Selective Cleaning enabled. Processing {len(rows_to_process_pre)} rows with errors. Skipping {len(skipped_rows_pre)} valid rows.")
        
        if not rows_to_process_pre:
            # No rows need cleaning
            return initial_list, None
        
        # We still need to respect MAX_AI_ROWS for the rows we DO process
        cleaned_rows = rows_to_process_pre
        externally_skipped = skipped_rows_pre
    else:
        externally_skipped = []

    # --- Smart Prioritization ---
    # We want to process rows that have errors first.
    def get_row_priority(r):
        idx = r.get("__rowIndex")
        has_error = idx in row_errors
        return (not has_error, idx)

    # Stable sort: Errors first, then rowIndex
    cleaned_rows.sort(key=get_row_priority)
    
    rows_to_process = cleaned_rows
    skipped_rows = externally_skipped
    
    if len(cleaned_rows) > MAX_AI_ROWS:
        print(f"WARN: AI Clean request limited to top {MAX_AI_ROWS} priority rows (out of {len(cleaned_rows)}).")
        rows_to_process = cleaned_rows[:MAX_AI_ROWS]
        skipped_rows = cleaned_rows[MAX_AI_ROWS:] + externally_skipped
    
    row_meta_all = build_row_meta_for_prompt(template_key, tpl, rows_to_process, row_errors)
    
    # --- Parallel Batching Implementation ---
    BATCH_SIZE = 50
    final_output_rows = []
    
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_cost = 0.0

    import concurrent.futures

    # ... (helper remains same, just need to change the loop target to rows_to_process)

    # Prepare Structured Output Schema
    cleaning_schema = get_json_schema_for_cleaning(template_key, tpl, include_pii_in_ai)

    # Helper function for processing a single batch
    def process_ai_batch(batch_rows, batch_index):
        if not batch_rows:
            return [], None, []
        
        # Filter row_meta for this batch
        batch_indices = {r["__rowIndex"] for r in batch_rows}
        batch_meta = [m for m in row_meta_all if m["__rowIndex"] in batch_indices]

        # Build prompt
        prompt = build_clean_prompt(
            template_key=template_key,
            tpl=tpl,
            rows=batch_rows,
            row_meta=batch_meta,
            settings=settings,
            full_dataset_context=full_dataset_context,
            extras=extras,
            include_pii_in_ai=include_pii_in_ai,
        )

        # Call OpenAI
        print(f"DEBUG: Starting AI batch {batch_index} ({len(batch_rows)} rows)")
        parsed_res, usage_res = call_openai_chat(
            prompt, 
            call_type="clean-values-schema", 
            tenant_id=tenant_id, 
            job_id=job_id,
            response_format=cleaning_schema
        )
        return parsed_res, usage_res, batch_rows

    # Prepare batches
    batches = []
    for i in range(0, len(rows_to_process), BATCH_SIZE):
        batch = rows_to_process[i : i + BATCH_SIZE]
        batches.append((batch, i))

    if not batches:
        return cleaned_rows, None

    # Run batches in parallel
    MAX_WORKERS = 10
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ai_batch, b[0], b[1]): b[1] for b in batches}
        
        results = []
        # Collect results as they complete
        for future in concurrent.futures.as_completed(futures):
            batch_start_idx = futures[future]
            try:
                parsed_res, usage_res, original_batch_rows = future.result()
                
                # Accumulate Usage
                if usage_res:
                    total_prompt_tokens += usage_res.get("prompt_tokens", 0)
                    total_completion_tokens += usage_res.get("completion_tokens", 0)
                    total_cost += estimate_cost_from_usage(usage_res)

                # Process Output
                batch_output = []
                if parsed_res and isinstance(parsed_res, dict) and isinstance(parsed_res.get("outputRows"), list):
                    # Get PII field keys
                    pii_field_keys = set()
                    if not include_pii_in_ai:
                        for f in tpl["fields"]:
                            if f.get("is_pii", False):
                                pii_field_keys.add(f["key"])
                    
                    for r in parsed_res.get("outputRows"):
                        if "__rowIndex" not in r:
                            continue
                        
                        idx = int(r["__rowIndex"])
                        # Find original row to preserve PII fields that weren't sent to AI
                        orig = next((row for row in original_batch_rows if row.get("__rowIndex") == idx), None)
                        
                        if not orig:
                            continue

                        out_row = {"__rowIndex": idx}
                        for f in tpl["fields"]:
                            fk = f["key"]
                            if fk in pii_field_keys and not include_pii_in_ai:
                                out_row[fk] = orig.get(fk, "")
                            else:
                                if only_fix_invalid:
                                    # Strict check: If field is valid, keep original value.
                                    # Handle both int/str keys for row_errors lookup
                                    bad_fields = row_errors.get(idx) or row_errors.get(str(idx)) or []
                                    if fk not in bad_fields:
                                        out_row[fk] = orig.get(fk, "")
                                    else:
                                        out_row[fk] = r.get(fk, "")
                                else:
                                    out_row[fk] = r.get(fk, "")
                        batch_output.append(out_row)
                else:
                    # Fallback for this batch
                    print(f"WARN: Batch starting at {batch_start_idx} failed or invalid. Reverting to original.")
                    batch_output = original_batch_rows
                
                results.extend(batch_output)

            except Exception as exc:
                print(f"ERROR: Batch starting at {batch_start_idx} generated an exception: {exc}")
                # Fallback: find batch in 'batches' list
                fallback_batch = next((b[0] for b in batches if b[1] == batch_start_idx), [])
                results.extend(fallback_batch)

    # Merge skipped rows back
    results.extend(skipped_rows)

    # Sort results by rowIndex to restore order
    results.sort(key=lambda x: x["__rowIndex"])
    final_output_rows = results

    ai_usage_summary = {
        "promptTokens": total_prompt_tokens,
        "completionTokens": total_completion_tokens,
        "totalTokens": total_prompt_tokens + total_completion_tokens,
        "estimatedCost": total_cost,
    }
    
    cleaned_rows = final_output_rows

    # --- Airtight Fallback and Validation Pass ---
    # We re-run the core validation on AI output to ensure enums are valid.
    import random
    for row in cleaned_rows:
        for f in tpl["fields"]:
            fk = f["key"]
            val = row.get(fk)
            
            # Re-clean using the core logic to detect errors/invalid enums
            clean_val, status = clean_value(
                template_key, f, val, 
                valid_ref_ids=valid_ref_ids, 
                session_context=full_dataset_context, 
                all_templates=None # Will use local templates if needed? No, sebaiknya pass.
            )
            
            # If invalid (status 'bad' means it failed pattern/enum check)
            if status == "bad":
                if settings and settings.get("ensureNoEmptyValues"):
                    # Fill with fallback/random as requested
                    allowed = f.get("allowed")
                    pattern = f.get("pattern")
                    
                    if allowed and isinstance(allowed, list) and len(allowed) > 0:
                        # Pick a random valid enum if the AI's was wrong
                        row[fk] = random.choice(allowed)
                    elif pattern == "date":
                        row[fk] = "01-01-1980"
                    elif pattern == "integer":
                        row[fk] = "100"
                    elif "name" in fk.lower() or "firstName" in fk or "surname" in fk:
                        row[fk] = "Blankid"
                    elif "email" in fk.lower():
                        fname = str(row.get("firstName", "user")).lower()
                        sname = str(row.get("surname", "name")).lower()
                        row[fk] = f"{fname}.{sname}@example.com"
                    else:
                        row[fk] = "(blank)"
                else:
                    # Blank it if invalid and not 'ensureNoEmptyValues'
                    row[fk] = ""
            else:
                # Update with cleaned/canonical value
                row[fk] = clean_val

    return cleaned_rows, ai_usage_summary


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat() + "Z"})



@app.route("/api/tenant-data/search", methods=["GET"])
def api_search_tenant_data():
    """
    Search and filter tenant data.
    Params:
      - tenantId (optional, defaults to demo-tenant if not in header/param, but let's say query param for now)
      - template (optional): filter by templateKey (e.g. 'Bookings')
      - q (optional): text search across all fields
      - status (optional): filter by status field if it exists
      - limit (optional): max records
      - skip (optional): pagination
    """
    # For POC, just grab generic tenant
    tenant_id = request.args.get("tenantId", "demo-tenant")
    template_key = request.args.get("template") or request.args.get("templateKey")
    query_text = request.args.get("q", "").strip()
    status_filter = request.args.get("status")
    
    limit = int(request.args.get("limit", 50))
    skip = int(request.args.get("skip", 0))

    coll = get_tenant_data_collection()
    if coll is None:
        return jsonify({"error": "DB not connected"}), 500

    # Build Mongo Query
    mongo_query = {"tenantId": tenant_id}
    
    if template_key:
        mongo_query["templateKey"] = {"$regex": f"^{re.escape(template_key)}$", "$options": "i"}

    # Search logic (Text)
    if query_text:
        regex = {"$regex": query_text, "$options": "i"}
        if template_key == "Bookings":
             mongo_query["$or"] = [
                 {"data.bookingRef": regex},
                 {"data.assessmentName": regex},
                 {"data.providerName": regex},
                 {"data.locationName": regex},
                 {"data._parentRef_personNumber.label": regex}
             ]
        elif template_key == "People":
             mongo_query["$or"] = [
                 {"data.firstName": regex},
                 {"data.surname": regex},
                 {"data.email": regex},
                 {"data.id": regex},
             ]
        else:
             pass

    # Generic Field Filtering (e.g. &status=Pending -> data.status=Pending)
    # We iterate over all args and if they match a known field in the template (or just generic data param), we add it.
    # To be safe, we'll look for arguments that are NOT the standard ones.
    reserved_args = {"tenantId", "template", "templateKey", "q", "limit", "skip"}
    
    for k, v in request.args.items():
        if k not in reserved_args and v:
            # Assume it's a data field filter
            # We treat numeric strings as likely numbers if the schema says so, but for now strict string match or simple conversion?
            # Mongo finds are type-sensitive.
            
            # Special handling: if value looks like integer, try matching both string and int?
            # or just rely on what's in DB.
            
            if v.lower() == "true":
                val = True
            elif v.lower() == "false":
                val = False
            elif v.isdigit():
                 # Match string OR number for robustness
                 val = {"$in": [v, int(v)]}
            else:
                 val = v
            
            mongo_query[f"data.{k}"] = val

    total_count = coll.count_documents(mongo_query)
    
    cursor = coll.find(mongo_query).sort("timestamp", -1).skip(skip).limit(limit)
    
    results = []
    for doc in cursor:
        item = doc.get("data", {})
        item["_id"] = str(doc["_id"])
        item["_templateKey"] = doc.get("templateKey")
        item["_createdAt"] = doc.get("timestamp")
        results.append(item)

    # --- Child Record Lookahead ---
    # optimization: check if any other templates reference this one.
    # If so, count how many children each result item has.
    
    # 1. Identify child templates and their join fields
    child_refs = []
    for t_key, t_def in TEMPLATES.items():
        refs = t_def.get("references", {})
        for ref_field, ref_cfg in refs.items():
             if ref_cfg.get("targetTemplate") == template_key:
                 child_refs.append({
                     "childTemplate": t_key,
                     "childRefField": ref_field, # e.g. personNumber
                     "parentTargetField": ref_cfg.get("targetField") # e.g. id
                 })
    
    if child_refs and results:
        # 2. For each child relation, agg count
        for cr in child_refs:
            parent_field = cr["parentTargetField"]
            
            # Collect all IDs from the current result page
            parent_ids = set()
            for r in results:
                val = r.get(parent_field)
                if val:
                    parent_ids.add(val)
            
            if not parent_ids:
                continue

            # Aggregation: Match specific children, Group by ref field
            pipeline = [
                {
                    "$match": {
                        "tenantId": tenant_id,
                        "templateKey": cr["childTemplate"],
                        f"data.{cr['childRefField']}": {"$in": list(parent_ids)}
                    }
                },
                {
                    "$group": {
                        "_id": f"$data.{cr['childRefField']}",
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            agg_res = list(coll.aggregate(pipeline))
            
            # Map counts back to results
            count_map = {str(item["_id"]): item["count"] for item in agg_res}
            
            for r in results:
                p_val = str(r.get(parent_field))
                if p_val in count_map:
                    if "_childCounts" not in r:
                        r["_childCounts"] = {}
                    r["_childCounts"][cr["childTemplate"]] = count_map[p_val]

    return jsonify({
        "data": results,
        "meta": {
            "total": total_count,
            "limit": limit,
            "skip": skip
        }
    })


# --- Header mapping (rule-based) -------------------------------------------

@app.route("/api/import/upload", methods=["POST"])
def api_import_upload():
    if "file" not in request.files:
        return jsonify({"error": "CSV file is required"}), 400

    file = request.files["file"]
    raw = file.read().decode("utf-8", errors="ignore")

    headers, _ = parse_csv_content(raw)
    if not headers:
        return jsonify({"error": "File is empty or invalid CSV"}), 400

    detected_key = detect_template_from_headers(headers)
    mappings = suggest_header_mappings(detected_key, headers, current_mapping={})

    suggested_mapping = {}
    mapping_sources = {}
    for m in mappings:
        suggested_mapping[m["templateKey"]] = m["matchedHeader"]
        mapping_sources[m["templateKey"]] = m["source"]

    templates_list = []
    for k, v in TEMPLATES.items():
        templates_list.append({"key": k, "label": v["label"], "fields": v["fields"]})

    return jsonify(
        {
            "headers": headers,
            "detected_template": detected_key,
            "suggested_mapping": suggested_mapping,
            "mapping_sources": mapping_sources,
            "templates": templates_list,
        }
    )


@app.route("/api/import/preview", methods=["POST"])
def api_import_preview():
    """End-to-end server-side preview of a CSV import in a single call."""
    body = request.get_json(force=True, silent=False) or {}
    csv_text = body.get("csvText")
    if not csv_text:
        return jsonify({"error": "csvText is required"}), 400

    tenant_id = body.get("tenantId")
    requested_template_key = body.get("templateKey")
    existing_mapping = body.get("existingMapping") or {}
    use_ai = bool(body.get("useAi", False))
    settings = body.get("settings") or {}
    full_dataset_context = body.get("fullDatasetContext") or None

    headers, data_rows = parse_csv_content(csv_text)
    if not headers:
        return jsonify({"error": "CSV is empty or invalid"}), 400

    all_templates = get_templates(tenant_id)

    if requested_template_key:
        try:
            tpl = get_template(requested_template_key, all_templates)
            template_key = tpl["key"]
        except ValueError as ex:
            return jsonify({"error": str(ex)}), 400
    else:
        template_key = detect_template_from_headers(headers, all_templates)
        tpl = get_template(template_key, all_templates)

    mappings = suggest_header_mappings(template_key, headers, existing_mapping)

    suggested_mapping = {}
    mapping_sources = {}
    for m in mappings:
        suggested_mapping[m["templateKey"]] = m["matchedHeader"]
        mapping_sources[m["templateKey"]] = m["source"]

    mapped_rows = build_template_rows_from_csv(template_key, headers, data_rows, suggested_mapping, all_templates)

    # Pre-fetch valid IDs
    valid_ref_ids = get_valid_ref_ids(template_key, all_templates, tenant_id=tenant_id, session_context=full_dataset_context)

    # 1) algorithmic clean
    cleaned_rows_initial, row_errors_initial, inferred_ids_initial = clean_rows_for_template(template_key, mapped_rows, all_templates, tenant_id=tenant_id, session_context=full_dataset_context, valid_ref_ids=valid_ref_ids)

    # 2) optional AI pass
    cleaned_rows = cleaned_rows_initial
    row_errors = row_errors_initial
    inferred_ids = inferred_ids_initial
    ai_usage_summary = None

    if use_ai:
        extras = {"headerMapping": suggested_mapping}
        cleaned_rows_ai, ai_usage_summary = enhance_rows_with_ai(
            template_key=template_key,
            tpl=tpl,
            cleaned_rows=cleaned_rows_initial,
            row_errors=row_errors_initial,
            tenant_id=tenant_id,
            job_id=None,
            use_ai=True,
            settings=settings,
            full_dataset_context=full_dataset_context,
            extras=extras,
            valid_ref_ids=valid_ref_ids
        )
        cleaned_rows, row_errors, inferred_ids = clean_rows_for_template(template_key, cleaned_rows_ai, all_templates, tenant_id=tenant_id, session_context=full_dataset_context, valid_ref_ids=valid_ref_ids)

    if template_key == "PatientData":
        for r in cleaned_rows:
            if not r.get("blockName"):
                r["blockName"] = "Block1"

    output_rows = []
    for r in cleaned_rows:
        obj = {"__rowIndex": r["__rowIndex"]}
        for f in tpl["fields"]:
            obj[f["key"]] = r.get(f["key"], "")
        output_rows.append(obj)

    return jsonify(
        {
            "headers": headers,
            "templateKey": template_key,
            "templateLabel": tpl["label"],
            "suggestedMapping": suggested_mapping,
            "mappingSources": mapping_sources,
            "previewRows": output_rows,
            "rowErrors": {str(k): v for k, v in row_errors.items()},
            "aiUsage": ai_usage_summary,
            "inferredIds": inferred_ids,
            "totalRows": len(output_rows),
        }
    )


@app.route("/api/import/ai/header-mapping", methods=["POST"])
def api_header_mapping():
    data = request.get_json(force=True, silent=False) or {}
    template_key = data.get("templateKey")
    uploaded_headers = data.get("uploadedHeaders") or data.get("headers") or []
    current_mapping_list = data.get("currentMapping") or []
    use_ai = bool(data.get("useAi", True))
    tenant_id = data.get("tenantId")

    # Extract enhanced data from frontend
    template_fields = data.get("templateFields")
    template_label = data.get("templateLabel")
    template_keywords = data.get("templateKeywords")
    sample_data = data.get("sampleData")

    if not template_key:
        return jsonify({"error": "templateKey is required"}), 400

    try:
        all_templates = get_templates(tenant_id)
        tpl = get_template(template_key, all_templates)
    except ValueError as ex:
        return jsonify({"error": str(ex)}), 400

    # Normalise current mapping into dict[templateKey -> header or None]
    current_mapping: dict[str, str] = {}
    for m in current_mapping_list:
        if not isinstance(m, dict):
            continue
        tkey = m.get("templateKey")
        h = m.get("matchedHeader") or m.get("uploadedHeader")
        if tkey:
            current_mapping[tkey] = h

    # Build sample_rows from sampleData (list of dicts) if provided
    sample_rows_for_mapping = None
    if sample_data and isinstance(sample_data, list) and len(sample_data) > 0 and isinstance(sample_data[0], dict):
        sample_rows_for_mapping = sample_data

    # ---- Run the 4-tier waterfall (T1→T2→T3→T4) ----
    waterfall_mappings = waterfall_header_match(
        template_key=template_key,
        uploaded_headers=uploaded_headers,
        tenant_id=tenant_id,
        current_mapping=current_mapping,
        all_templates=all_templates,
        sample_rows=sample_rows_for_mapping,
    )
    waterfall_by_key = {m["templateKey"]: m for m in waterfall_mappings}

    matched_count = sum(1 for m in waterfall_mappings if m.get("matchedHeader"))
    print(f"WATERFALL: {matched_count}/{len(waterfall_mappings)} fields matched for {template_key}")

    # If AI is disabled, no key configured, or waterfall already got ≥80% confidence → skip AI
    if not (use_ai and OPENAI_API_KEY):
        return jsonify({"mappings": waterfall_mappings, "aiUsage": None})

    total_fields = len(waterfall_mappings)
    high_conf = [m for m in waterfall_mappings if m.get("matchedHeader") and m.get("confidence", 0) >= 0.5]
    if total_fields > 0 and len(high_conf) / total_fields >= 0.80:
        print(f"WATERFALL: Skipping AI — {len(high_conf)}/{total_fields} fields already high-confidence")
        return jsonify({"mappings": waterfall_mappings, "aiUsage": None})

    # Build enhanced prompt with enum values and sample data
    prompt = build_header_prompt(
        template_key=template_key,
        uploaded_headers=uploaded_headers,
        current_mapping=current_mapping,
        template_fields=template_fields,
        template_label=template_label,
        template_keywords=template_keywords,
        sample_data=sample_data,
        all_templates=all_templates
    )

    print(f"DEBUG: api_header_mapping calling AI for {template_key} ({total_fields - len(high_conf)} unmapped fields)")

    parsed, usage = call_openai_chat(prompt, call_type="header-mapping", tenant_id=tenant_id, job_id=None)

    ai_usage_summary = None
    final_mappings = None

    if parsed and isinstance(parsed, dict) and isinstance(parsed.get("mappings"), list):
        parsed_list = parsed["mappings"]
        used_headers = set()
        final_mappings = []

        for field in tpl["fields"]:
            fk = field["key"]

            ai_item = next((m for m in parsed_list if m.get("templateKey") == fk), None)
            matched = ai_item.get("matchedHeader") if ai_item else None
            try:
                conf = float(ai_item.get("confidence", 0.0)) if ai_item else 0.0
            except Exception:
                conf = 0.0

            if matched not in uploaded_headers:
                matched = None
                conf = 0.0

            if matched and matched in used_headers:
                matched = None
                conf = 0.0

            # Prefer waterfall result if AI is unconfident
            wfall_m = waterfall_by_key.get(fk)
            if (not matched or conf < 0.5) and wfall_m and wfall_m.get("matchedHeader"):
                matched = wfall_m["matchedHeader"]
                conf = max(conf, float(wfall_m.get("confidence", 0.0)))
                source = f"waterfall-fallback({wfall_m.get('source','rule')})"
            else:
                source = "ai" if matched else "ai-none"

            if matched:
                used_headers.add(matched)

            final_mappings.append(
                {"templateKey": fk, "matchedHeader": matched, "confidence": conf, "source": source}
            )

        if usage:
            ai_usage_summary = {
                "promptTokens": usage.get("prompt_tokens", 0),
                "completionTokens": usage.get("completion_tokens", 0),
                "totalTokens": usage.get("total_tokens", 0),
                "estimatedCost": estimate_cost_from_usage(usage),
            }

    if not final_mappings:
        final_mappings = waterfall_mappings

    print(f"DEBUG: api_header_mapping returning {len(final_mappings)} mappings")
    return jsonify({"mappings": final_mappings, "aiUsage": ai_usage_summary})


@app.route("/api/import/save-header-aliases", methods=["POST"])
def api_save_header_aliases():
    """
    Tier-2 Tenant Memory: persist confirmed header→field mappings.
    Called fire-and-forget from the frontend after a successful import confirm.

    Body: { tenantId, aliases: [{ templateKey, fieldKey, uploadedHeader }] }
    """
    data = request.get_json(force=True, silent=False) or {}
    tenant_id = data.get("tenantId")
    aliases = data.get("aliases") or []

    if not tenant_id:
        return jsonify({"error": "tenantId required"}), 400
    if not aliases:
        return jsonify({"saved": 0})

    try:
        coll = get_header_aliases_collection()
        if coll is None:
            return jsonify({"error": "DB unavailable"}), 503

        saved = 0
        for a in aliases:
            tpl_key = a.get("templateKey")
            field_key = a.get("fieldKey")
            uploaded_header = a.get("uploadedHeader")
            if not (tpl_key and field_key and uploaded_header):
                continue
            try:
                coll.update_one(
                    {
                        "tenantId": tenant_id,
                        "templateKey": tpl_key,
                        "fieldKey": field_key,
                        "uploadedHeader": uploaded_header,
                    },
                    {"$set": {"tenantId": tenant_id, "templateKey": tpl_key,
                              "fieldKey": field_key, "uploadedHeader": uploaded_header,
                              "confirmedAt": __import__("datetime").datetime.utcnow()}},
                    upsert=True,
                )
                saved += 1
            except Exception:
                pass  # duplicate or write error — non-fatal

        print(f"WATERFALL T2: Saved {saved} header aliases for tenant {tenant_id}")
        return jsonify({"saved": saved})

    except Exception as e:
        print(f"ERROR: save-header-aliases: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/header-aliases", methods=["GET"])
def api_list_header_aliases():
    """
    List all saved T2 header aliases for a tenant.
    Returns aliases grouped by templateKey.
    Query params: ?tenantId=...
    """
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId required"}), 400
    try:
        coll = get_header_aliases_collection()
        if coll is None:
            return jsonify({"error": "DB unavailable"}), 503
        docs = list(coll.find(
            {"tenantId": tenant_id},
            {"_id": 0, "tenantId": 0}
        ).sort([("templateKey", 1), ("fieldKey", 1)]))
        # Convert datetime to ISO string for JSON serialisation
        for d in docs:
            if "confirmedAt" in d and hasattr(d["confirmedAt"], "isoformat"):
                d["confirmedAt"] = d["confirmedAt"].isoformat()
        return jsonify({"aliases": docs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/header-aliases", methods=["DELETE"])
def api_delete_header_alias():
    """
    Delete a single T2 header alias.
    Body: { tenantId, templateKey, fieldKey, uploadedHeader }
    """
    data = request.get_json(force=True, silent=False) or {}
    tenant_id  = data.get("tenantId")
    tpl_key    = data.get("templateKey")
    field_key  = data.get("fieldKey")
    uploaded_h = data.get("uploadedHeader")

    if not all([tenant_id, tpl_key, field_key, uploaded_h]):
        return jsonify({"error": "tenantId, templateKey, fieldKey, uploadedHeader all required"}), 400
    try:
        coll = get_header_aliases_collection()
        if coll is None:
            return jsonify({"error": "DB unavailable"}), 503
        result = coll.delete_one({
            "tenantId": tenant_id,
            "templateKey": tpl_key,
            "fieldKey": field_key,
            "uploadedHeader": uploaded_h,
        })
        if result.deleted_count == 0:
            return jsonify({"error": "Alias not found"}), 404
        return jsonify({"deleted": 1})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/jobs/<job_id>/export-errors", methods=["GET"])
def api_export_job_errors(job_id):
    """
    Download a CSV of all rows that failed validation/processing for a job.
    Each row has the original data fields plus an __error column.
    Query params: ?tenantId=...
    """
    import csv
    import io
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId required"}), 400
    try:
        coll_records = get_ingestion_records_collection()
        coll_jobs    = get_ingestion_jobs_collection()
        if coll_records is None:
            return jsonify({"error": "DB unavailable"}), 503

        # Security: ensure job belongs to tenant
        job = coll_jobs.find_one({"jobId": job_id, "tenantId": tenant_id}) if coll_jobs is not None else None
        if job is None:
            return jsonify({"error": "Job not found"}), 404

        error_docs = list(coll_records.find(
            {"jobId": job_id, "status": "error"},
            {"_id": 0, "data": 1, "error": 1, "rowIndex": 1, "templateKey": 1}
        ).sort("rowIndex", 1))

        if not error_docs:
            return jsonify({"error": "No error rows for this job"}), 404

        # Build unified column set
        all_keys = []
        seen_keys = set()
        for doc in error_docs:
            for k in (doc.get("data") or {}).keys():
                if k not in seen_keys:
                    all_keys.append(k)
                    seen_keys.add(k)

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=["__rowIndex", "__template", "__error"] + all_keys,
            extrasaction="ignore"
        )
        writer.writeheader()
        for doc in error_docs:
            row = {
                "__rowIndex": doc.get("rowIndex", ""),
                "__template": doc.get("templateKey", ""),
                "__error":    doc.get("error", ""),
                **(doc.get("data") or {}),
            }
            writer.writerow(row)

        csv_bytes = output.getvalue().encode("utf-8")
        short_id = job_id[:8]
        return app.response_class(
            csv_bytes,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=errors_{short_id}.csv"}
        )
    except Exception as e:
        print(f"ERROR: export-errors: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/jobs/<job_id>/rollback", methods=["POST"])
def api_rollback_job(job_id):
    """
    Roll back a completed job:
      - DELETE tenant_data records where jobId = job_id AND __operation = "created"
      - CLEAR jobId tag from records where __operation = "updated" (data change cannot be undone)
      - Mark ingestion_records as rolled_back
      - Mark job as rolled_back

    Body: { tenantId }
    Returns: { deleted, cleared, warning }
    """
    from datetime import datetime as _dt
    data = request.get_json(force=True, silent=False) or {}
    tenant_id = data.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId required"}), 400

    try:
        coll_jobs    = get_ingestion_jobs_collection()
        coll_records = get_ingestion_records_collection()
        coll_data    = get_tenant_data_collection()

        if coll_jobs is None or coll_records is None or coll_data is None:
            return jsonify({"error": "DB unavailable"}), 503

        # Verify job belongs to this tenant and is in a rollback-able state
        job = coll_jobs.find_one({"jobId": job_id, "tenantId": tenant_id})
        if not job:
            return jsonify({"error": "Job not found"}), 404
        if job.get("status") not in ("completed", "error"):
            return jsonify({"error": f"Cannot roll back a job with status '{job.get('status')}'"}), 409

        # 1. Delete records that were CREATED by this job
        del_result = coll_data.delete_many({
            "tenantId": tenant_id,
            "jobId": job_id,
            "__operation": "created",
        })
        deleted = del_result.deleted_count

        # 2. Clear jobId from records that were UPDATED (data can't be restored)
        upd_result = coll_data.update_many(
            {"tenantId": tenant_id, "jobId": job_id, "__operation": "updated"},
            {"$unset": {"jobId": ""}, "$set": {"__rollbackNote": f"Updated by rolled-back job {job_id}"}},
        )
        cleared = upd_result.modified_count

        # 3. Mark ingestion_records as rolled_back
        coll_records.update_many(
            {"jobId": job_id},
            {"$set": {"status": "rolled_back"}}
        )

        # 4. Mark job as rolled_back
        coll_jobs.update_one(
            {"_id": job["_id"]},
            {"$set": {
                "status": "rolled_back",
                "rolledBackAt": _dt.utcnow(),
                "rollbackSummary": {"deleted": deleted, "cleared": cleared},
            }}
        )

        warning = None
        if cleared > 0:
            warning = (
                f"{cleared} record(s) were updates to existing data and cannot be fully reversed. "
                "Their jobId tag has been cleared but the data changes remain."
            )

        print(f"ROLLBACK: Job {job_id} — deleted {deleted} created records, cleared {cleared} updated records")
        return jsonify({"deleted": deleted, "cleared": cleared, "warning": warning})

    except Exception as e:
        print(f"ERROR: rollback: {e}")
        return jsonify({"error": str(e)}), 500


def detect_templates_with_ai(headers: list, sample_data: list, tenant_id: str, all_templates: dict = None) -> list:
    """
    Ask GPT which template(s) match the given headers/data.
    Returns list of template keys.
    """
    if not (OPENAI_API_KEY and headers):
        # Fallback to heuristic
        heuristic = detect_template_from_headers(headers, all_templates)
        return [heuristic] if heuristic else []

    source = all_templates if all_templates is not None else TEMPLATES
    candidates = []
    for k, v in source.items():
        candidates.append({
            "key": k,
            "label": v.get("label", k),
            "keywords": v.get("keywords", [])
        })

    prompt_obj = {
        "goal": "Identify which CSV template(s) best match the uploaded headers and sample data.",
        "candidates": candidates,
        "uploadedHeaders": headers,
        "sampleData": sample_data[:5] if sample_data else [],
        "instructions": [
            "Return a JSON object with a single key 'matchedTemplates' containing a list of template keys.",
            "If multiple templates match (e.g. mixed data or ambiguous), return all of them.",
            "If no template matches well, return an empty list.",
            "Prefer exact matches on keywords and field names."
        ]
    }
    
    prompt = json.dumps(prompt_obj, indent=2)
    parsed, _ = call_openai_chat(prompt, call_type="detect-template", tenant_id=tenant_id)
    
    if parsed and isinstance(parsed, dict):
        return parsed.get("matchedTemplates", [])
    
    return []


@app.route("/api/import/ai/detect-and-map", methods=["POST"])

def api_ai_detect_and_map():
    data = request.get_json(force=True, silent=False) or {}
    uploaded_headers = data.get("uploadedHeaders") or data.get("headers") or []
    sample_data = data.get("sampleData") or []
    tenant_id = data.get("tenantId")
    allow_multi = data.get("allowMultiTemplates", True)

    if not uploaded_headers:
        return jsonify({"error": "No headers provided"}), 400

    # 1. Template Detection (Ask AI for potentially multiple templates, or use provided)
    all_templates = get_templates(tenant_id)
    
    detected_keys = []
    if not allow_multi and data.get("templateKey"):
        # If multi-templates disabled and user already has a template selected,
        # skip AI detection and just use that template for mapping.
        detected_keys = [data["templateKey"]]
    else:
        detected_keys = detect_templates_with_ai(uploaded_headers, sample_data, tenant_id, all_templates)
        if not allow_multi and detected_keys:
            detected_keys = detected_keys[:1]


    # 2. Map Headers for EACH detected template
    results_by_template = {}
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_estimated_cost = 0.0

    import concurrent.futures

    def process_single_template_mapping(template_key):
        try:
            tpl = get_template(template_key, all_templates)
        except ValueError:
            return None, None, None

        # Prepare fields for mapping
        template_fields = []
        for f in tpl["fields"]:
            field_obj = {
                "key": f["key"],
                "label": f.get("label", ""),
                "required": bool(f.get("required", False)),
                "isPii": bool(f.get("is_pii", False)),
                "description": f.get("label", ""),
            }
            if f.get("allowed"):
                field_obj["allowedValues"] = f.get("allowed")
            template_fields.append(field_obj)

        # Run mapping
        # 0. Rule-Based Fallback (always compute)
        rule_mappings = suggest_header_mappings(template_key, uploaded_headers, current_mapping={}, all_templates=all_templates)
        rule_by_key = {m["templateKey"]: m for m in rule_mappings}

        # 1. AI Mapping (optional)
        prompt = build_header_prompt(
            template_key=template_key,
            uploaded_headers=uploaded_headers,
            current_mapping={},
            template_fields=template_fields,
            template_label=tpl.get("label"),
            template_keywords=tpl.get("keywords"),
            sample_data=sample_data,
            all_templates=all_templates
        )
        print(f"DEBUG: api_ai_detect_and_map calling OpenAI for template {template_key}")

        parsed, usage = call_openai_chat(prompt, call_type="header-mapping", tenant_id=tenant_id)
        
        # 2. Merge AI & Rule-Based
        final_mappings = []
        
        # Helper to get AI confidence/match
        ai_matches = {}
        if parsed and isinstance(parsed, dict) and isinstance(parsed.get("mappings"), list):
            for m in parsed["mappings"]:
                    if m.get("templateKey"):
                        ai_matches[m["templateKey"]] = m

        used_headers = set()
        
        for field in tpl["fields"]:
            fk = field["key"]
            
            # AI Suggestion
            ai_item = ai_matches.get(fk)
            matched = ai_item.get("matchedHeader") if ai_item else None
            conf = float(ai_item.get("confidence", 0.0)) if ai_item else 0.0
            
            # Validation: must be in uploaded_headers
            if matched not in uploaded_headers:
                matched = None
                conf = 0.0
            
            # Validation: must be unique
            if matched and matched in used_headers:
                matched = None
                conf = 0.0
            
            # Fallback to Rule if AI is weak/missing
            rule_m = rule_by_key.get(fk)
            if (not matched or conf < 0.5) and rule_m and rule_m.get("matchedHeader"):
                matched = rule_m["matchedHeader"]
                conf = max(conf, float(rule_m.get("confidence", 0.0)))
                source = "rule-fallback"
            else:
                source = "ai" if matched else "ai-none"
            
            if matched:
                used_headers.add(matched)
                
            final_mappings.append({
                "templateKey": fk, 
                "matchedHeader": matched, 
                "confidence": conf, 
                "source": source
            })

        return template_key, final_mappings, usage

    # Run in parallel
    MAX_WORKERS = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_template_mapping, tk): tk for tk in detected_keys}
        
        for future in concurrent.futures.as_completed(futures):
            tk = futures[future]
            try:
                res_key, res_mappings, res_usage = future.result()
                if res_key:
                    results_by_template[res_key] = {
                        "templateKey": res_key,
                        "mappings": res_mappings
                    }
                    if res_usage:
                        total_prompt_tokens += res_usage.get("prompt_tokens", 0)
                        total_completion_tokens += res_usage.get("completion_tokens", 0)
                        total_estimated_cost += estimate_cost_from_usage(res_usage)
            except Exception as exc:
                 print(f"ERROR: Template mapping for {tk} generated an exception: {exc}")

    # Consolidated Usage
    ai_usage_summary = {
        "promptTokens": total_prompt_tokens,
        "completionTokens": total_completion_tokens,
        "totalTokens": total_prompt_tokens + total_completion_tokens,
        "estimatedCost": total_estimated_cost,
    }

    return jsonify({
        "detectedTemplateKeys": detected_keys,
        "results": results_by_template,
        "aiUsage": ai_usage_summary
    })


# --------------------------------------------------------------------------
# Auth Endpoints
# --------------------------------------------------------------------------

@app.route("/api/auth/login", methods=["POST"])
def api_login():
    """Simple login that returns user document from DB."""
    body = request.get_json(force=True, silent=True) or {}
    user_id = body.get("userId")
    
    if not user_id:
        return jsonify({"error": "userId is required"}), 400
    
    coll = get_users_collection()
    user = coll.find_one({"userId": user_id})
    
    if not user:
        return jsonify({"error": "Invalid user ID"}), 401
    
    user["_id"] = str(user["_id"])
    return jsonify({"user": user})

@app.route("/api/auth/me", methods=["GET"])
@require_auth
def api_get_me():
    """Return current user info based on X-User-ID header."""
    user = getattr(request, 'user', None)
    if user:
        user["_id"] = str(user["_id"])
    return jsonify({"user": user})


@app.route("/api/tenants", methods=["GET"])
@require_auth
def api_get_tenants():
    """Return a list of available tenants for the current user."""
    """List all available tenants for the current user."""
    # We use get_current_user_id() from auth_utils
    user_id = get_current_user_id()
    user = getattr(request, 'user', None) # require_auth attaches it
    
    # Static list for POC
    tenants = [
        {"id": "acme-corp", "name": "Acme Corp"},
        {"id": "globex", "name": "Globex Corp"},
        {"id": "stark-ind", "name": "Stark Industries"}
    ]

    # If super admin, return all
    if user_id == "admin-1":
        return jsonify({"tenants": tenants})

    # Otherwise filter by user association
    if not user:
        return jsonify({"tenants": []})
        
    accessible_ids = [t.get("tenantId") for t in user.get("tenants", [])]
    accessible = [t for t in tenants if t["id"] in accessible_ids]
            
    return jsonify({"tenants": accessible})





@app.route("/api/import/ai/clean", methods=["POST"])
def api_clean_values():
    """Clean + normalise rows for a given template, with optional AI assistance."""
    body = request.get_json(force=True, silent=False) or {}
    template_key = body.get("templateKey")
    rows = body.get("rows") or []
    use_ai = bool(body.get("useAi", False))
    tenant_id = body.get("tenantId")
    settings = body.get("settings") or {}
    full_dataset_context = body.get("fullDatasetContext") or None

    extras = {
        "rowMeta": body.get("rowMeta"),
        "headerMapping": body.get("headerMapping"),
        "fieldSamples": body.get("fieldSamples"),
        "sequenceHints": body.get("sequenceHints"),
    }

    if not template_key:
        return jsonify({"error": "templateKey is required"}), 400

    all_templates = get_templates(tenant_id)
    try:
        tpl = get_template(template_key, all_templates)
    except ValueError as ex:
        return jsonify({"error": str(ex)}), 400

    # Pre-fetch valid IDs for relationship validation
    valid_ref_ids = get_valid_ref_ids(template_key, all_templates, tenant_id=tenant_id, session_context=full_dataset_context)

    # 1) algorithmic clean
    cleaned_rows_initial, row_errors_initial, inferred_ids_initial = clean_rows_for_template(
        template_key, rows, all_templates, tenant_id=tenant_id, session_context=full_dataset_context, valid_ref_ids=valid_ref_ids
    )

    # 2) AI pass (optional)
    cleaned_rows = cleaned_rows_initial
    row_errors = row_errors_initial
    inferred_ids = inferred_ids_initial
    ai_usage_summary = None
    
    include_pii_in_ai = bool(body.get("includePiiInAi", True))

    if use_ai:
        cleaned_rows_ai, ai_usage_summary = enhance_rows_with_ai(
            template_key=template_key,
            tpl=tpl,
            cleaned_rows=cleaned_rows_initial,
            row_errors=row_errors_initial,
            tenant_id=tenant_id,
            job_id=None,
            use_ai=True,
            settings=settings,
            full_dataset_context=full_dataset_context,
            extras=extras,
            include_pii_in_ai=include_pii_in_ai,
        )
        cleaned_rows, row_errors, inferred_ids = clean_rows_for_template(
            template_key, cleaned_rows_ai, all_templates, tenant_id=tenant_id, session_context=full_dataset_context, valid_ref_ids=valid_ref_ids
        )

    if template_key == "PatientData":
        for r in cleaned_rows:
            if not r.get("blockName"):
                r["blockName"] = "Block1"

    output_rows = []
    for r in cleaned_rows:
        obj = {"__rowIndex": r["__rowIndex"]}
        for f in tpl["fields"]:
            obj[f["key"]] = r.get(f["key"], "")
        output_rows.append(obj)

    return jsonify(
        {
            "outputRows": output_rows,
            "rowErrors": {str(k): v for k, v in row_errors.items()},
            "aiUsage": ai_usage_summary,
            "inferredIds": inferred_ids,
        }
    )


@app.route("/api/import/save", methods=["POST"])
def api_save_import():
    """Persist cleaned data to MongoDB."""
    body = request.get_json(force=True, silent=False) or {}
    tenant_id = body.get("tenantId")
    template_key = body.get("templateKey")
    rows = body.get("rows") or []

    if not tenant_id or not template_key:
        return jsonify({"error": "tenantId and templateKey are required"}), 400

    coll_imports = get_imports_collection()
    coll_data = get_tenant_data_collection()

    if coll_imports is None or coll_data is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    import_id = str(uuid.uuid4())
    timestamp = datetime.utcnow()

    # 1. Save import metadata
    coll_imports.insert_one({
        "importId": import_id,
        "tenantId": tenant_id,
        "templateKey": template_key,
        "rowCount": len(rows),
        "timestamp": timestamp
    })

    # 2. Save individual records with tenant_id for easy retrieval/filtering
    # We add metadata to each record for multi-tenancy isolation at the data level
    records_to_save = []
    for r in rows:
        record = {
            "importId": import_id,
            "tenantId": tenant_id,
            "templateKey": template_key,
            "data": r,
            "timestamp": timestamp
        }
        records_to_save.append(record)

    if records_to_save:
        coll_data.insert_many(records_to_save)

    return jsonify({
        "message": "Data saved successfully",
        "importId": import_id,
        "rowCount": len(rows)
    })


@app.route("/api/import/dump", methods=["POST"])
def api_import_dump():
    """Store pre-cleaned data from frontend (already validated and edited by user)."""
    body = request.get_json(force=True, silent=False) or {}
    tenant_id = body.get("tenantId")
    template_key = body.get("templateKey")
    rows = body.get("rows") or []
    file_name = body.get("fileName", "unknown.csv")

    if not tenant_id or not template_key:
        return jsonify({"error": "tenantId and templateKey are required"}), 400

    coll_raw = get_raw_uploads_collection()
    if coll_raw is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    upload_id = str(uuid.uuid4())
    doc = {
        "uploadId": upload_id,
        "tenantId": tenant_id,
        "templateKey": template_key,
        "cleanedRows": rows,  # Pre-cleaned by frontend (not raw CSV)
        "preCleaned": True,   # Flag to indicate data is already validated/cleaned
        "fileName": file_name,
        "uploadedAt": datetime.utcnow()
    }
    res = coll_raw.insert_one(doc)
    print(f"DEBUG: Inserted raw upload into DB: {coll_raw.database.name}, Collection: {coll_raw.name}. UploadId: {upload_id}, InsertedId: {res.inserted_id}")

    return jsonify({
        "message": "Raw data dumped successfully",
        "uploadId": upload_id,
        "rowCount": len(rows)
    })


@app.route("/api/import/trigger-job", methods=["POST"])
@require_role(ROLE_EDITOR)
def api_import_trigger_job():
    """Trigger an ingestion job for MULTIPLE dumped uploads with dependency-aware planning."""
    body = request.get_json(force=True, silent=False) or {}
    
    # Support both single and multiple for backward compatibility
    upload_ids = body.get("uploadIds")
    if not upload_ids:
        single_id = body.get("uploadId")
        if single_id:
            upload_ids = [single_id]

    tenant_id = body.get("tenantId")
    
    print(f"DEBUG: api_import_trigger_job called for tenant {tenant_id}")
    # Force reload
    print(f"DEBUG: uploadIds requested: {upload_ids}")

    if not upload_ids or not isinstance(upload_ids, list):
        return jsonify({"error": "uploadIds list is required"}), 400

    coll_jobs = get_ingestion_jobs_collection()
    coll_raw = get_raw_uploads_collection()
    coll_records = get_ingestion_records_collection()

    if coll_jobs is None or coll_raw is None or coll_records is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    # 1. Fetch ALL raw uploads
    # We need to map TemplateKey -> UploadDoc to plan correctly
    upload_docs = list(coll_raw.find({"uploadId": {"$in": upload_ids}}))
    
    if len(upload_docs) != len(upload_ids):
        return jsonify({"error": f"One or more uploads not found. Found {len(upload_docs)} of {len(upload_ids)}"}), 404

    # Validate they all belong to the same tenant (if strict)
    # And gather template keys
    affected_templates = []
    template_to_upload_id = {}
    
    for doc in upload_docs:
        t_key = doc.get("templateKey")
        if t_key:
            affected_templates.append(t_key)
            template_to_upload_id[t_key] = doc["uploadId"]
    
    print(f"DEBUG: TriggerJob - Affected Templates: {affected_templates}")
    print(f"DEBUG: TriggerJob - Template to Upload Mapping: {template_to_upload_id}")
    for k, v in template_to_upload_id.items():
        print(f"DEBUG: TriggerJob - Mapping Detail: '{k}' -> '{v}' (type: {type(v)})")

    # 2. Plan Job
    all_templates = get_templates(tenant_id)
    try:
        # Calculate order for the combined set of templates
        execution_order = get_execution_order(affected_templates, all_templates)
        print(f"DEBUG: TriggerJob - Execution Order: {execution_order}")
    except ValueError as e:
        print(f"DEBUG: Dependency Sorting ERROR: {e}")
        return jsonify({"error": f"Dependency Cycle: {e}"}), 400
    except Exception as e:
        print(f"DEBUG: Unexpected error in get_execution_order: {e}")
        return jsonify({"error": f"Internal sorting error: {e}"}), 500

    stages = {}
    for t_key in execution_order:
        # Find which upload provides this template
        u_id = template_to_upload_id.get(t_key)
        
        if not u_id:
            print(f"DEBUG: TriggerJob - Skipping template '{t_key}' in plan because no upload provides it.")
            continue

        # Get row count from the specific upload doc
        u_doc = next((d for d in upload_docs if d["uploadId"] == u_id), None)
        row_count = len(u_doc.get("cleanedRows") or u_doc.get("rawRows", [])) if u_doc else 0

        stages[t_key] = {
            "status": "pending",
            "blockedBy": TEMPLATES.get(t_key, {}).get("dependencies", []),
            "uploadId": u_id, # CRITICAL: Link stage to specific upload
            "rowCount": row_count
        }

    print(f"DEBUG: TriggerJob - Final Stages Config: {list(stages.keys())}")
    for st, cfg in stages.items():
        print(f"DEBUG: TriggerJob - Stage '{st}' uploadId: '{cfg.get('uploadId')}' (type: {type(cfg.get('uploadId'))})")

    job_id = str(uuid.uuid4())
    job_doc = {
        "jobId": job_id,
        "uploadIds": upload_ids, # Store all
        "tenantId": tenant_id or upload_docs[0].get("tenantId"),
        "status": "pending",
        "jobPlan": {
            "executionOrder": execution_order,
            "stages": stages
        },
        "triggeredAt": datetime.utcnow(),
        "completedAt": None,
        "error": None
    }
    
    print(f"DEBUG: TriggerJob - FINAL job_doc jobId: {job_id}, stages: {list(job_doc['jobPlan']['stages'].keys())}")
    for k, v in job_doc['jobPlan']['stages'].items():
         print(f"DEBUG: TriggerJob - FINAL Stage '{k}' uploadId: '{v.get('uploadId')}'")
    
    res = coll_jobs.insert_one(job_doc)
    print(f"DEBUG: Created Job {job_id} in DB.")

    # 3. Create Row-Level Ingestion Records for ALL uploads
    ingestion_records = []
    
    for u_doc in upload_docs:
        u_id = u_doc["uploadId"]
        t_key = u_doc.get("templateKey")
        cleaned_rows = u_doc.get("cleanedRows") or u_doc.get("rawRows", [])
        
        for idx, row in enumerate(cleaned_rows):
            rec = {
                "jobId": job_id,
                "uploadId": u_id,
                "tenantId": tenant_id,
                "templateKey": t_key,
                "rowIndex": idx,
                "status": "pending",
                "data": row,
                "preCleaned": u_doc.get("preCleaned", False),
                "errors": [],
                "resolution": {
                    "entityId": None,
                    "parentResolved": False
                },
                "createdAt": datetime.utcnow()
            }
            ingestion_records.append(rec)
    
    if ingestion_records:
        coll_records.insert_many(ingestion_records)
        print(f"DEBUG: Inserted {len(ingestion_records)} ingestion records for Job {job_id}")

    return jsonify({
        "message": "Ingestion job triggered successfully",
        "jobId": job_id,
        "uploadIds": upload_ids,
        "plan": job_doc["jobPlan"]
    })


@app.route("/api/data/search", methods=["GET"])
def api_search_data():
    """Keyed search + pagination for large datasets."""
    tenant_id = request.args.get("tenantId")
    template_key = request.args.get("templateKey")
    try:
        limit = int(request.args.get("limit", 50))
        skip = int(request.args.get("skip", 0))
    except ValueError:
        limit = 50
        skip = 0

    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400

    coll_data = get_tenant_data_collection()
    if coll_data is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    query = {"tenantId": tenant_id}
    if template_key:
        query["templateKey"] = template_key

    total = coll_data.count_documents(query)
    cursor = coll_data.find(query).sort("timestamp", -1).skip(skip).limit(limit)

    results = []
    for doc in cursor:
        ts = doc.get("timestamp")
        if isinstance(ts, datetime):
            ts = ts.isoformat()
        
        results.append({
            "id": str(doc["_id"]),
            "data": doc.get("data", {}),
            "timestamp": ts,
            "templateKey": doc.get("templateKey", ""),
            "__operation": doc.get("__operation", "")
        })

    return jsonify({
        "data": results,
        "total": total,
        "page": (skip // limit) + 1,
        "limit": limit
    })


@app.route("/api/data", methods=["GET"])
def api_get_data():
    """Retrieve saved data for a tenant."""
    tenant_id = request.args.get("tenantId")
    template_key = request.args.get("templateKey")

    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400

    coll_data = get_tenant_data_collection()
    if coll_data is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    query = {"tenantId": tenant_id}
    if template_key:
        query["templateKey"] = template_key
    
    # If jobId is provided, we want to see records *processed* by this job, 
    # even if they are currently "owned" by a newer job (due to upserts).
    # detailed lookup via ingestion_records.
    job_id = request.args.get("jobId")
    if job_id:
        coll_records = get_ingestion_records_collection()
        # Get all resolved records for this job
        job_records = list(coll_records.find(
            {"jobId": job_id, "status": "resolved"},
            {"data": 1, "templateKey": 1, "processedData": 1} # Projection
        ))
        
        # Build list of identifiers to fetch current state
        # For now, we assume 'id' is the primary key as per template or fallback to '_id' logic if we had it.
        # But broadly, we can search matches.
        # Optimization: Group by templateKey
        
        results = []
        if not job_records:
             return jsonify({"data": []})

        # Process by template for efficiency
        # We assume the user is viewing one type or we aggregate.
        # But SavedDataViewer usually iterates everything.
        
        # To avoid complex N+1, let's just grab the identifiers (e.g. 'id') from the ingestion_records
        # and query tenant_data for them.
        
        # 1. Extract IDs or Snapshot Data from ingestion records
        lookups_by_template = {} # { (templateKey, idField): [vals] }
        snapshot_results = []
        
        all_templates = get_templates(tenant_id)
        
        for r in job_records:
            if "processedData" in r:
                # ... already handled
                processed_data = r["processedData"]
                operation = processed_data.get("__operation", "")
                data_without_operation = {k: v for k, v in processed_data.items() if k != "__operation"}
                
                snapshot_results.append({
                    "id": str(r["_id"]),
                    "data": data_without_operation,
                    "timestamp": datetime.now().isoformat(), 
                    "template_key": r.get("templateKey", ""),
                    "templateKey": r.get("templateKey", ""), # support both camel/snake
                    "__operation": operation
                })
            else:
                tk = r.get("templateKey")
                d = r.get("data", {})
                if tk and d:
                    td = all_templates.get(tk)
                    if td:
                        pk_fields = [f["key"] for f in td["fields"] if f.get("identifier")]
                        pk = pk_fields[0] if pk_fields else "id"
                        val = d.get(pk)
                        if val:
                            lookups_by_template.setdefault((tk, pk), []).append(val)
        
        if snapshot_results:
            results.extend(snapshot_results)
        
        if lookups_by_template:
            # Query tenant_data for each template/pk combination
            for (tk, pk), vals in lookups_by_template.items():
                q = {"tenantId": tenant_id, "templateKey": tk}
                q[f"data.{pk}"] = {"$in": vals}
                cursor = coll_data.find(q).limit(2000)
                for doc in cursor:
                    results.append({
                        "id": str(doc["_id"]),
                        "data": doc["data"],
                        "timestamp": doc["timestamp"].isoformat() if isinstance(doc["timestamp"], datetime) else doc["timestamp"],
                        "templateKey": doc["templateKey"],
                        "__operation": doc.get("__operation", "")
                    })
            
        print(f"DEBUG: Returning {len(results)} results")
        return jsonify({"data": results})

    # Default / Legacy behavior (if no jobId, or for general browser)
    # Fetch latest imports first (limit increased to 1000)
    cursor = coll_data.find(query).sort("timestamp", -1).limit(1000)
    
    results = []
    for doc in cursor:
        results.append({
            "id": str(doc["_id"]),
            "data": doc["data"],
            "timestamp": doc["timestamp"].isoformat() if isinstance(doc["timestamp"], datetime) else doc["timestamp"],
            "templateKey": doc["templateKey"],
            "__operation": doc.get("__operation", "")
        })

    return jsonify({"data": results})


@app.route("/api/jobs", methods=["GET"])
@require_role(ROLE_VIEWER)
def api_list_jobs():
    """List ingestion jobs for a tenant."""
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400

    coll_jobs = get_ingestion_jobs_collection()
    if coll_jobs is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    # Filter out legacy jobs (created before 2026-02-06 11:00 UTC) that don't have linked data
    cutoff_date = datetime(2026, 2, 6, 11, 0, 0)
    query = {
        "tenantId": tenant_id,
        "triggeredAt": {"$gt": cutoff_date}
    }
    
    cursor = coll_jobs.find(query).sort("triggeredAt", -1).limit(50)
    jobs = []
    
    for doc in cursor:
        # Convert datetime to string
        doc["_id"] = str(doc["_id"])
        if doc.get("triggeredAt"):
            doc["triggeredAt"] = doc["triggeredAt"].isoformat() + "Z"
        if doc.get("completedAt"):
            doc["completedAt"] = doc["completedAt"].isoformat() + "Z"
        jobs.append(doc)

    return jsonify({"jobs": jobs})


@app.route("/api/jobs/<job_id>", methods=["GET"])
@require_role(ROLE_VIEWER)
def api_get_job(job_id):
    """Get full details/plan for a specific job."""
    coll_jobs = get_ingestion_jobs_collection()
    if coll_jobs is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    job = coll_jobs.find_one({"jobId": job_id})
    if not job:
        return jsonify({"error": "Job not found"}), 404

    job["_id"] = str(job["_id"])
    if job.get("triggeredAt"):
        job["triggeredAt"] = job["triggeredAt"].isoformat() + "Z"
    if job.get("completedAt"):
        job["completedAt"] = job["completedAt"].isoformat() + "Z"
        
    return jsonify({"job": job})


@app.route("/api/jobs/<job_id>/records", methods=["GET"])
@require_role(ROLE_VIEWER)
def api_get_job_records(job_id):
    """Get row-level records for a job."""
    coll_records = get_ingestion_records_collection()
    if coll_records is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    filter_query = {"jobId": job_id}
    status = request.args.get("status")
    if status:
        filter_query["status"] = status

    cursor = coll_records.find(filter_query).sort("rowIndex", 1).limit(200)
    records = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        if doc.get("createdAt"):
            doc["createdAt"] = doc["createdAt"].isoformat() + "Z"
        records.append(doc)

    return jsonify({"records": records})


@app.route("/api/jobs/<job_id>/trace", methods=["GET"])
@require_role(ROLE_VIEWER)
def api_download_job_trace(job_id):
    """Generate and return a plain-text execution trace log for a job."""
    from flask import Response
    coll_jobs = get_ingestion_jobs_collection()
    coll_records = get_ingestion_records_collection()
    if coll_jobs is None or coll_records is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    job = coll_jobs.find_one({"jobId": job_id})
    if not job:
        return jsonify({"error": "Job not found"}), 404

    tenant_id = job.get("tenantId", "unknown")
    triggered_at = job.get("triggeredAt")
    start_ts = triggered_at.isoformat() + "Z" if triggered_at else "unknown"

    # Fetch all records in topological order (by stage then rowIndex)
    execution_order = job.get("jobPlan", {}).get("executionOrder", [])
    all_records = list(coll_records.find({"jobId": job_id}).sort("rowIndex", 1))

    # Sort records by executionOrder stage priority then rowIndex
    def stage_sort_key(rec):
        tpl = rec.get("templateKey", "")
        try:
            return (execution_order.index(tpl), rec.get("rowIndex", 0))
        except ValueError:
            return (len(execution_order), rec.get("rowIndex", 0))

    all_records.sort(key=stage_sort_key)

    # Track failed parent IDs per template for cascade detection
    # Map: templateKey -> set of entity IDs that failed
    failed_ids = {}  # { templateKey: set(id_value) }

    lines = []
    total = len(all_records)
    counts = {"success": 0, "failed": 0, "skipped": 0}

    for seq, rec in enumerate(all_records, start=1):
        tpl_key = rec.get("templateKey", "?")
        row_data = rec.get("data", {})
        status_raw = rec.get("status", "pending")
        err_msg = rec.get("error", "")
        processed = rec.get("processedData", {})

        # Determine entity ID (first identifier-like field or rowIndex)
        entity_id = (
            row_data.get("id") or row_data.get("bookingRef") or
            row_data.get("patientId") or str(rec.get("rowIndex", seq))
        )

        # Determine parent ref (relationship field value)
        parent_ref = (
            row_data.get("personNumber") or row_data.get("patientId") or "none"
        ) if tpl_key != "People" else "none"

        # Determine timestamp for this record
        rec_ts = rec.get("updatedAt") or rec.get("createdAt") or triggered_at
        ts_str = rec_ts.strftime("%H:%M:%S.%f")[:12] if hasattr(rec_ts, "strftime") else "??:??:??.???"

        # Check if parent failed → SKIPPED cascade
        skipped_msg = None
        if tpl_key != "People" and parent_ref and parent_ref != "none":
            # Find which template the parent belongs to (first stage dependency)
            for dep_tpl, failed_set in failed_ids.items():
                if str(parent_ref) in failed_set:
                    skipped_msg = f"Parent Entity ({parent_ref}) failed."
                    break

        if skipped_msg:
            status_label = "SKIPPED"
            msg = skipped_msg
            counts["skipped"] += 1
        elif status_raw == "resolved":
            op = processed.get("__operation", "updated")
            status_label = "SUCCESS"
            msg = "Insert complete." if op == "created" else "Upsert complete."
            counts["success"] += 1
        elif status_raw == "error":
            status_label = "FAILED "
            msg = err_msg or "Unknown error."
            counts["failed"] += 1
            # Track this as a failed parent for cascade detection
            if tpl_key not in failed_ids:
                failed_ids[tpl_key] = set()
            failed_ids[tpl_key].add(str(entity_id))
        else:
            status_label = "PENDING"
            msg = "Not yet processed."

        line = (
            f"[{seq:03d}] [{ts_str}] [{status_label}] {tpl_key:<12} | "
            f"ID: {str(entity_id):<8} | REF: {str(parent_ref):<8} | MSG: {msg}"
        )
        lines.append(line)

    SEP = "=" * 70
    header = "\n".join([
        SEP,
        "JOB EXECUTION TRACE",
        SEP,
        f"Job ID:      {job_id}",
        f"Tenant:      {tenant_id}",
        f"Start Time:  {start_ts}",
        f"Records:     {total}",
        SEP,
        "",
    ])
    body = "\n".join(lines)
    footer = "\n".join([
        "",
        SEP,
        f"SUMMARY: {total} Attempted | {counts['success']} Success | {counts['failed']} Failed | {counts['skipped']} Skipped",
        SEP,
        "",
    ])

    log_text = header + body + footer

    filename = f"job_trace_{job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    return Response(
        log_text,
        mimetype="text/plain",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )



# --------------------------------------------------------------------------
# Webhook API Endpoints
# --------------------------------------------------------------------------

@app.route("/api/webhooks", methods=["GET"])
@require_role(ROLE_ADMIN)
def api_list_webhooks():
    """List all webhooks for a tenant."""
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400

    coll = get_webhooks_collection()
    if coll is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    docs = list(coll.find({"tenantId": tenant_id}).sort("createdAt", 1))
    for d in docs:
        d["_id"] = str(d["_id"])
        if d.get("createdAt"):
            d["createdAt"] = d["createdAt"].isoformat() + "Z"
        if d.get("updatedAt"):
            d["updatedAt"] = d["updatedAt"].isoformat() + "Z"

    return jsonify({"webhooks": docs})


@app.route("/api/webhooks", methods=["POST"])
@require_role(ROLE_ADMIN)
def api_create_webhook():
    """Create a new webhook for a tenant."""
    body = request.get_json(force=True, silent=True) or {}
    tenant_id = body.get("tenantId")
    url = (body.get("url") or "").strip()
    name = (body.get("name") or url).strip()

    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400
    if not url:
        return jsonify({"error": "url is required"}), 400

    coll = get_webhooks_collection()
    if coll is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    now = datetime.utcnow()
    webhook_id = str(uuid.uuid4())
    doc = {
        "webhookId": webhook_id,
        "tenantId": tenant_id,
        "name": name,
        "url": url,
        "active": bool(body.get("active", True)),
        "entityTypes": body.get("entityTypes") or [],
        "oauth": {
            "enabled": bool((body.get("oauth") or {}).get("enabled", False)),
            "tokenUrl": ((body.get("oauth") or {}).get("tokenUrl") or "").strip(),
            "clientId": ((body.get("oauth") or {}).get("clientId") or "").strip(),
            "clientSecret": ((body.get("oauth") or {}).get("clientSecret") or "").strip(),
            "scope": ((body.get("oauth") or {}).get("scope") or "").strip(),
        },
        "createdAt": now,
        "updatedAt": now,
    }
    coll.insert_one(doc)
    doc["_id"] = str(doc["_id"])
    doc["createdAt"] = now.isoformat() + "Z"
    doc["updatedAt"] = now.isoformat() + "Z"

    return jsonify({"message": "Webhook created", "webhook": doc}), 201


@app.route("/api/webhooks/<webhook_id>", methods=["PUT"])
@require_role(ROLE_ADMIN)
def api_update_webhook(webhook_id):
    """Update an existing webhook."""
    body = request.get_json(force=True, silent=True) or {}

    coll = get_webhooks_collection()
    if coll is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    existing = coll.find_one({"webhookId": webhook_id})
    if not existing:
        return jsonify({"error": "Webhook not found"}), 404

    now = datetime.utcnow()
    updates = {"updatedAt": now}

    if "name" in body:
        updates["name"] = (body["name"] or "").strip()
    if "url" in body:
        url = (body["url"] or "").strip()
        if not url:
            return jsonify({"error": "url cannot be empty"}), 400
        updates["url"] = url
    if "active" in body:
        updates["active"] = bool(body["active"])
    if "entityTypes" in body:
        updates["entityTypes"] = body["entityTypes"] or []
    if "oauth" in body:
        oauth_in = body["oauth"] or {}
        updates["oauth"] = {
            "enabled": bool(oauth_in.get("enabled", False)),
            "tokenUrl": (oauth_in.get("tokenUrl") or "").strip(),
            "clientId": (oauth_in.get("clientId") or "").strip(),
            "clientSecret": (oauth_in.get("clientSecret") or "").strip(),
            "scope": (oauth_in.get("scope") or "").strip(),
        }

    coll.update_one({"webhookId": webhook_id}, {"$set": updates})
    updated = coll.find_one({"webhookId": webhook_id})
    updated["_id"] = str(updated["_id"])
    if updated.get("createdAt"):
        updated["createdAt"] = updated["createdAt"].isoformat() + "Z"
    updated["updatedAt"] = now.isoformat() + "Z"

    return jsonify({"message": "Webhook updated", "webhook": updated})


@app.route("/api/webhooks/<webhook_id>", methods=["DELETE"])
@require_role(ROLE_ADMIN)
def api_delete_webhook(webhook_id):
    """Delete a webhook."""
    coll = get_webhooks_collection()
    if coll is None:
        return jsonify({"error": "MongoDB not connected"}), 500

    result = coll.delete_one({"webhookId": webhook_id})
    if result.deleted_count == 0:
        return jsonify({"error": "Webhook not found"}), 404

    return jsonify({"message": "Webhook deleted"})


# --------------------------------------------------------------------------
# Template Builder API Endpoints
# --------------------------------------------------------------------------

@app.route("/api/templates", methods=["GET"])
@require_role(ROLE_VIEWER) # Viewers can see templates
def api_get_templates():
    """Get all templates for a tenant (custom + defaults)."""
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
    
    result = {"templates": list(all_templates.values())}
    print(f"DEBUG: Returning templates response: {type(result)}, keys: {all_templates.keys()}, count: {len(result['templates'])}")
    return jsonify(result)



@app.route("/api/templates", methods=["POST"])
@require_role(ROLE_ADMIN)
def api_save_template():
    """Create a new custom template."""
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
        "locked": body.get("locked", False),
        "createdAt": datetime.utcnow(),
        "updatedAt": datetime.utcnow(),
        "createdBy": body.get("createdBy", "unknown")
    }
    
    result = coll_templates.insert_one(template_doc)
    template_doc["_id"] = str(result.inserted_id)
    template_doc["createdAt"] = template_doc["createdAt"].isoformat() + "Z"
    template_doc["updatedAt"] = template_doc["updatedAt"].isoformat() + "Z"
    
    return jsonify({"message": "Template created successfully", "template": template_doc}), 201


@app.route("/api/templates/<template_key>", methods=["GET"])
def api_get_template(template_key):
    """Get a single template by key."""
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


@app.route("/api/templates/<template_key>", methods=["PUT"])
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
    if "locked" in body:
        update_data["locked"] = body["locked"]
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
    
    return jsonify({"message": "Template updated successfully", "template": updated})


@app.route("/api/templates/<template_key>", methods=["DELETE"])
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
        force_delete = request.args.get("force", "false").lower() == "true"
        
        if data_count > 0:
            if not force_delete:
                return jsonify({
                    "error": f"Cannot delete template '{template_key}' because {data_count} records exist. Delete the data first or confirm force delete."
                }), 409
            else:
                # Force delete: remove the data first
                coll_data.delete_many({"tenantId": tenant_id, "templateKey": template_key})

    # Delete template
    coll_templates.delete_one({"tenantId": tenant_id, "templateKey": template_key})
    
    return jsonify({"message": f"Template '{template_key}' deleted successfully"})


@app.route("/api/admin/reset-data", methods=["DELETE"])
def api_reset_tenant_data():
    """Clear all data for a tenant (except templates)."""
    tenant_id = request.args.get("tenantId")
    if not tenant_id:
        return jsonify({"error": "tenantId is required"}), 400

    pwd = request.args.get("password")
    # Simple safety check (in real app, use proper auth)
    # limit to localhost or specific secret if needed, but for POC just strict param
    if not pwd or pwd != "secret-reset": 
         # We can relax this for the POC or make it a simple prompt in UI
         pass 

    try:
        from db_config import (
            get_tenant_data_collection, 
            get_ingestion_records_collection,
            get_ingestion_jobs_collection,
            get_raw_uploads_collection,
            get_imports_collection
        )
        
        # 1. Tenant Data
        c_data = get_tenant_data_collection()
        r1 = c_data.delete_many({"tenantId": tenant_id})
        
        # 2. Ingestion Records (jobs row data)
        # We need to find jobs for this tenant first to be safe, 
        # or just delete by generic if we store tenantId on records (we don't always).
        # Actually records have jobId. Jobs have tenantId.
        
        c_jobs = get_ingestion_jobs_collection()
        jobs = list(c_jobs.find({"tenantId": tenant_id}, {"jobId": 1}))
        job_ids = [j["jobId"] for j in jobs]
        
        c_records = get_ingestion_records_collection()
        r2 = c_records.delete_many({"jobId": {"$in": job_ids}})
        
        # 3. Jobs
        r3 = c_jobs.delete_many({"tenantId": tenant_id})
        
        # 4. Uploads (Optional)
        c_uploads = get_raw_uploads_collection()
        r4 = c_uploads.delete_many({"tenantId": tenant_id})
        
        # 5. Imports (Legacy)
        c_imports = get_imports_collection()
        r5 = c_imports.delete_many({"tenantId": tenant_id})

        return jsonify({
            "message": f"Reset complete for {tenant_id}",
            "deleted": {
                "tenant_data": r1.deleted_count,
                "records": r2.deleted_count,
                "jobs": r3.deleted_count,
                "uploads": r4.deleted_count
            }
        })
        
    except Exception as e:
        print(f"RESET ERROR: {e}")
        return jsonify({"error": str(e)}), 500


# --------------------------------------------------------------------------
# Background Worker: Job Processing (Imported from processing_worker.py)
# --------------------------------------------------------------------------
from processing_worker import process_ingestion_jobs




MOCK_DATA_CACHE = {}

@app.route("/api/ai/generate_mock_data", methods=["POST"])
def api_generate_mock_data():
    """Generate mock data based on template schema using AI."""
    body = request.get_json(force=True, silent=False) or {}
    fields = body.get("fields", [])
    
    if not fields:
        return jsonify({"error": "No fields provided"}), 400

    # Cache Check
    cache_key = None
    try:
        # Create a stable hash of the schema definition
        cache_key = hashlib.md5(json.dumps(fields, sort_keys=True).encode("utf-8")).hexdigest()
        if cache_key in MOCK_DATA_CACHE:
            print(f"DEBUG: Cache hit for mock data (Key: {cache_key})")
            cached_data = list(MOCK_DATA_CACHE[cache_key]) # copy
            random.shuffle(cached_data)
            return jsonify({"data": cached_data})
    except Exception as ex:
        print(f"DEBUG: Cache key generation failed: {ex}")

    if not OPENAI_API_KEY:
        return jsonify({"error": "OpenAI API key not configured"}), 503

    # Construct prompt
    schema_desc = []
    for f in fields:
        desc = f"Key: {f['key']} (Type: {f.get('pattern', 'string')})"
        if f.get('pattern') == 'enum' and f.get('allowed'):
            desc += f": {', '.join(f['allowed'][:5])}..."
        if f.get('label'):
            desc += f" - Content/Label: {f['label']}"
        schema_desc.append(desc)
    
    prompt = (
        "Generate 15 rows of realistic mock data for a CSV import file based on this schema:\n"
        + "\n".join(schema_desc)
        + "\n\nReturn ONLY a valid JSON array of objects. Do not wrap in markdown or code blocks. "
        "IMPORTANT: The keys in your JSON objects MUST MATCH the 'Key: ...' values exactly. Do not use the labels as keys."
    )

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        }
        payload = {
            "model": OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": "You are a data generator helper."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
        }
        
        resp = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=30)
        resp_json = resp.json()
        
        if "error" in resp_json:
            return jsonify({"error": str(resp_json["error"])}), 500
            
        content = resp_json["choices"][0]["message"]["content"]
        
        # Clean potential markdown
        content = content.replace("```json", "").replace("```", "").strip()
        
        data = json.loads(content)
        
        # Update Cache
        if cache_key and isinstance(data, list):
             MOCK_DATA_CACHE[cache_key] = data
             
        return jsonify({"data": data})

    except Exception as e:
        print(f"AI MOCK ERROR: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Start background worker only if we are in the reloader process (or if reloader is disabled)
    # When using use_reloader=True, the script is run twice. 
    # WERKZEUG_RUN_MAIN is set to 'true' in the child process.
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or os.environ.get("FLASK_DEBUG") != "1":
        print("MAIN: Starting background worker thread...")
        t = threading.Thread(target=process_ingestion_jobs, daemon=True)
        t.start()
    
    # Enable reloader for better DX
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True, use_reloader=True)