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
from dotenv import load_dotenv

load_dotenv()

import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
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
    get_templates_collection
)

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
# IMPORTANT: Do NOT hardcode secrets. Set OPENAI_API_KEY in your environment.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

app = Flask(__name__)
CORS(app, supports_credentials=True, resources={r"/api/*": {"origins": "*"}})

# Register REST API Blueprint
try:
    from api_data import data_bp
    app.register_blueprint(data_bp, url_prefix="/api/v1")
    print("Main: Registered data_bp at /api/v1")
except Exception as e:
    print(f"Main: Failed to register data_bp: {e}")

# Register GraphQL Endpoint
try:
    from flask_graphql import GraphQLView
    from schema_builder import schema
    
    # Custom View to inject tenant_id from query params into context
    class CustomGraphQLView(GraphQLView):
        def get_context(self):
            # Return a dict so we can inject values. 
            # info.context in resolvers will be this dict.
            return {
                'request': request,
                'tenant_id': request.args.get('tenantId')
            }

    app.add_url_rule(
        '/graphql',
        view_func=CustomGraphQLView.as_view(
            'graphql',
            schema=schema,
            graphiql=True # Enable GraphiQL interface
        )
    )
    print("Main: Registered GraphQL at /graphql")
except Exception as e:
    print(f"Main: Failed to register GraphQL: {e}")

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


def clean_value(template_key: str, field: dict, raw):
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
        required = bool(field.get("required", False))
        res = clean_generic_string(raw, required)

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


def clean_rows_for_template(template_key: str, rows: list, all_templates: dict = None):
    """
    Algorithmically clean + validate all rows for a given template.

    rows: [ { "__rowIndex": 0, "<fieldKey>": value, ... }, ... ]

    Returns:
      cleaned_rows: list[dict]
      row_errors: dict[rowIndex -> list[fieldKey]]
      inferred_ids: list[...]
    """
    tpl = get_template(template_key, all_templates)
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

            clean_val, status = clean_value(template_key, field, raw_val)

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
def compute_header_score(field: dict, header: str, all_templates: dict = None) -> float:
    """Heuristic score for mapping a given uploaded header to a template field."""
    if not header:
        return 0.0

    hl = header.lower()
    fl = field["label"].lower()
    fk = field["key"].lower()
    normh = normalise_header_string(header)
    normfl = normalise_header_string(field["label"])
    normfk = normalise_header_string(field["key"])

    score = 0.0

    if hl == fl or normh == normfl:
        score += 10.0
    if hl == fk or normh == normfk:
        score += 8.0
    if fl in hl or hl in fl:
        score += 5.0

    for syn in field.get("synonyms", []) or []:
        sl = syn.lower()
        norms = normalise_header_string(syn)
        if hl == sl or normh == norms:
            score += 9.0
        elif sl in hl or hl in sl:
            score += 4.0

    # small boost based on template keywords
    # If all_templates is not provided, it falls back to global TEMPLATES in get_template
    try:
        kw_list = get_template(field["templateKey"], all_templates)["keywords"]
        for kw in kw_list:
            if kw in hl:
                score += 1.5
    except ValueError:
        # If template not found (e.g. race condition or bad key), ignore keyword boost
        pass

    return score


def suggest_header_mappings(template_key: str, uploaded_headers: list, current_mapping: dict, all_templates: dict = None):
    """
    Pure rule-based header mapping suggestion.
    Returns list of {templateKey, matchedHeader, confidence, source}.
    """
    tpl = get_template(template_key, all_templates)
    current_mapping = current_mapping or {}
    used_headers = set(h for h in current_mapping.values() if h)
    mappings = []

    for field in tpl["fields"]:
        fkey = field["key"]
        base = {"templateKey": fkey, "matchedHeader": None, "confidence": 0.0, "source": "rule"}

        existing = current_mapping.get(fkey)
        if existing and existing in uploaded_headers:
            base["matchedHeader"] = existing
            base["confidence"] = 1.0
            mappings.append(base)
            used_headers.add(existing)
            continue

        best_score = 0.0
        best_header = None
        enriched_field = {**field, "templateKey": template_key}

        for h in uploaded_headers:
            if h in used_headers:
                continue
            score = compute_header_score(enriched_field, h, all_templates)
            if score > best_score:
                best_score = score
                best_header = h

        if best_header and best_score >= 5.0:
            base["matchedHeader"] = best_header
            base["confidence"] = min(best_score / 10.0, 1.0)
            used_headers.add(best_header)

        mappings.append(base)

    return mappings


def detect_template_from_headers(headers: list, all_templates: dict = None) -> str:
    """Lightweight heuristic to pick the most likely template based on headers."""
    detected_key = None
    best_score = 0
    
    source = all_templates if all_templates is not None else TEMPLATES

    for key, tpl in source.items():
        score = 0
        keywords = tpl.get("keywords") or []
        for kw in keywords:
            for h in headers:
                if kw in (h or "").lower():
                    score += 1
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
            clean_val, status = clean_value(template_key, f, val)
            
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

    # 1) algorithmic clean
    cleaned_rows_initial, row_errors_initial, inferred_ids_initial = clean_rows_for_template(template_key, mapped_rows, all_templates)

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
        )
        cleaned_rows, row_errors, inferred_ids = clean_rows_for_template(template_key, cleaned_rows_ai)

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

    # normalise current mapping into dict[templateKey -> header or None]
    current_mapping: dict[str, str] = {}
    for m in current_mapping_list:
        if not isinstance(m, dict):
            continue
        tkey = m.get("templateKey")
        h = m.get("matchedHeader") or m.get("uploadedHeader")
        if tkey:
            current_mapping[tkey] = h

    # Always compute rule-based suggestions as a safety net / fallback
    rule_mappings = suggest_header_mappings(template_key, uploaded_headers, current_mapping)
    rule_by_key = {m["templateKey"]: m for m in rule_mappings}
    
    print(f"DEBUG: rule_mappings count: {len(rule_mappings)}")
    # Log top 3 rule-based matches if any
    top_rules = [m for m in rule_mappings if m.get("matchedHeader")]
    if top_rules:
        print(f"DEBUG: top rule-based matches: {top_rules[:3]}")

    # If AI is disabled or no key configured, just return rule-based
    if not (use_ai and OPENAI_API_KEY):
        return jsonify({"mappings": rule_mappings, "aiUsage": None})

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
    
    print(f"DEBUG: api_header_mapping generated prompt for {template_key}")
    # print(f"DEBUG Prompt:\n{prompt}") # Uncomment if deep inspection is needed

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

            rule_m = rule_by_key.get(fk)
            if (not matched or conf < 0.5) and rule_m and rule_m.get("matchedHeader"):
                matched = rule_m["matchedHeader"]
                conf = max(conf, float(rule_m.get("confidence", 0.0)))
                source = "rule-fallback"
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

    print(f"DEBUG: api_header_mapping returning {len(final_mappings)} mappings")
    # print(f"Mappings: {final_mappings}")

    return jsonify({"mappings": final_mappings, "aiUsage": ai_usage_summary})


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


@app.route("/api/tenants", methods=["GET"])
def api_get_tenants():
    """Return a list of available tenants."""
    # In a real app, this might come from a DB. For now, we mock it.
    tenants = [
        {"id": "acme-corp", "name": "Acme Corp"},
        {"id": "globex", "name": "Globex Corporation"},
        {"id": "stark-ind", "name": "Stark Industries"},
    ]
    return jsonify({"tenants": tenants})





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

    # 1) algorithmic clean
    cleaned_rows_initial, row_errors_initial, inferred_ids_initial = clean_rows_for_template(template_key, rows, all_templates)

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
        cleaned_rows, row_errors, inferred_ids = clean_rows_for_template(template_key, cleaned_rows_ai, all_templates)

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
        return jsonify({"error": f"Dependency Cycle: {e}"}), 400

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



# --------------------------------------------------------------------------
# Template Builder API Endpoints
# --------------------------------------------------------------------------

@app.route("/api/templates", methods=["GET"])
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
def api_create_template():
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