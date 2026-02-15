
import requests
import json

BASE_URL = "http://localhost:5000"

def test_selective_clean():
    # Setup some rows: 1 valid, 1 invalid
    rows = [
        {"__rowIndex": 0, "id": "1", "type": "Candidate", "firstName": "John", "surname": "Doe", "gender": "M", "dob": "01-01-1980", "email": "john@example.com", "title": "Mr"},
        {"__rowIndex": 1, "id": "invalid", "type": "InvalidType", "firstName": "Jane", "surname": "Doe", "gender": "F", "dob": "01-01-1990", "email": "jane@example.com", "title": "Ms"}
    ]
    
    tenant_id = "test-tenant"
    
    # Pass 1: onlyFixInvalid=False (Clean All)
    print("Testing with onlyFixInvalid=False (Clean All)...")
    payload_all = {
        "templateKey": "People",
        "rows": rows,
        "useAi": True,
        "settings": {"onlyFixInvalid": False},
        "tenantId": tenant_id
    }
    resp_all = requests.post(f"{BASE_URL}/api/import/ai/clean", json=payload_all, timeout=60)
    usage_all = resp_all.json().get("aiUsage", {})
    print(f"Clean All Usage: {usage_all}")
    
    # Pass 2: onlyFixInvalid=True (Selective)
    print("Testing with onlyFixInvalid=True (Selective)...")
    payload_sel = {
        "templateKey": "People",
        "rows": rows,
        "useAi": True,
        "settings": {"onlyFixInvalid": True},
        "tenantId": tenant_id
    }
    resp_sel = requests.post(f"{BASE_URL}/api/import/ai/clean", json=payload_sel, timeout=60)
    usage_sel = resp_sel.json().get("aiUsage", {})
    print(f"Selective Usage: {usage_sel}")
    
    if usage_sel.get("promptTokens", 0) < usage_all.get("promptTokens", 0):
        print("VERIFICATION SUCCESS: Selective cleaning used fewer prompt tokens!")
    else:
        print("VERIFICATION FAILED: Selective cleaning did not reduce prompt tokens (maybe because dataset is small?)")

if __name__ == "__main__":
    # Start server in background if not running (already handling manually)
    test_selective_clean()

if __name__ == "__main__":
    test_selective_clean()
