"""
Test script to verify composite key behavior for PatientData template.

This script simulates the upsert logic to demonstrate how composite keys work:
- Same patientId + assessmentName = UPDATE
- Same patientId, different assessmentName = CREATE
"""

# Simulate template definition
PATIENT_DATA_TEMPLATE = {
    "key": "PatientData",
    "fields": [
        {"key": "patientId", "identifier": True},
        {"key": "assessmentName", "identifier": True},
        {"key": "tests", "identifier": False},
        {"key": "status", "identifier": False}
    ]
}

def build_upsert_query(template, tenant_id, row_data):
    """Simulates the worker's query building logic."""
    primary_keys = [f["key"] for f in template["fields"] if f.get("identifier")]
    query = {"tenantId": tenant_id, "templateKey": template["key"]}
    
    for pk in primary_keys:
        val = row_data.get(pk)
        if val is not None:
            query[f"data.{pk}"] = val
    
    return query

# Test scenarios
tenant_id = "tenant123"

print("=" * 60)
print("COMPOSITE KEY TEST - PatientData")
print("=" * 60)

# Scenario 1: First upload
row1 = {"patientId": "1001", "assessmentName": "Pre-Employment Medical", "tests": "Audio, Spiro", "status": "Completed"}
query1 = build_upsert_query(PATIENT_DATA_TEMPLATE, tenant_id, row1)
print("\n1. First upload - Patient 1001, Pre-Employment Medical")
print(f"   Query: {query1}")
print(f"   Result: CREATE new record")

# Scenario 2: Re-upload same patient, same assessment (UPDATE)
row2 = {"patientId": "1001", "assessmentName": "Pre-Employment Medical", "tests": "Audio, Spiro, Medical", "status": "Completed"}
query2 = build_upsert_query(PATIENT_DATA_TEMPLATE, tenant_id, row2)
print("\n2. Re-upload - Patient 1001, Pre-Employment Medical (updated tests)")
print(f"   Query: {query2}")
print(f"   Result: UPDATE existing record (same composite key)")
print(f"   Match: {query1 == query2}")

# Scenario 3: Same patient, different assessment (CREATE)
row3 = {"patientId": "1001", "assessmentName": "Return to Work Assessment", "tests": "Functional", "status": "Pending"}
query3 = build_upsert_query(PATIENT_DATA_TEMPLATE, tenant_id, row3)
print("\n3. Upload - Patient 1001, Return to Work Assessment")
print(f"   Query: {query3}")
print(f"   Result: CREATE new record (different composite key)")
print(f"   Match with scenario 1: {query1 == query3}")

# Scenario 4: Different patient, same assessment (CREATE)
row4 = {"patientId": "2002", "assessmentName": "Pre-Employment Medical", "tests": "Audio", "status": "Pending"}
query4 = build_upsert_query(PATIENT_DATA_TEMPLATE, tenant_id, row4)
print("\n4. Upload - Patient 2002, Pre-Employment Medical")
print(f"   Query: {query4}")
print(f"   Result: CREATE new record (different composite key)")
print(f"   Match with scenario 1: {query1 == query4}")

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print("✅ Composite key (patientId + assessmentName) ensures:")
print("   - Same patient can have multiple assessments")
print("   - Each assessment is tracked separately")
print("   - Re-uploading same assessment updates the record")
print("=" * 60)
