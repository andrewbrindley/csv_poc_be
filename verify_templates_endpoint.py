
import requests
import json
import sys

# Adjust URL if needed
API_URL = "http://localhost:5000/api/templates"
TENANT_ID = "acme-corp"

def check_templates():
    print(f"Checking templates for tenant: {TENANT_ID}")
    
    try:
        response = requests.get(f"{API_URL}?tenantId={TENANT_ID}")
        if response.status_code != 200:
            print(f"Error: API returned {response.status_code}")
            return False
            
        data = response.json()
        templates = data.get("templates", [])
        
        print(f"Total templates returned: {len(templates)}")
        
        system_keys = {"People", "Bookings", "PatientData"}
        found_keys = set()
        custom_found = False
        
        for t in templates:
            key = t.get('key') or t.get('templateKey')
            is_custom = "_id" in t
            found_keys.add(key)
            if is_custom:
                custom_found = True
                print(f"Found Custom Template: {key}")
        
        missing = system_keys - found_keys
        if missing:
            print(f"FAIL: Missing system templates: {missing}")
        else:
            print("SUCCESS: All system templates found.")
            
        if custom_found:
            print("SUCCESS: At least one custom template found.")
        else:
            print("INFO: No custom templates found (this might be expected if none created).")
            
        return True
        
    except Exception as e:
        print(f"Exception: {e}")
        return False

if __name__ == "__main__":
    check_templates()
