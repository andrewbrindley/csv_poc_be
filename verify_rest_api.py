
import requests
import json
import sys

BASE_URL = "http://localhost:5000/api/v1"

def run_test():
    print("--- STARTING API VERIFICATION ---")

    # 1. Setup
    tenant_id = "test_tenant_api_verify"
    resource = "people"
    
    # 2. Test Create (POST)
    print(f"\n1. Testing POST /{tenant_id}/{resource}...")
    new_person = {
        "firstName": "Rest",
        "surname": "Api",
        "email": "rest.api@example.com",
        "type": "Candidate",
        "title": "Dr",
        "gender": "M",
        "id": "99999"
    }
    
    try:
        resp = requests.post(f"{BASE_URL}/{tenant_id}/{resource}", json=new_person)
        print(f"Status: {resp.status_code}")
        print(f"Response: {resp.text}")
        
        if resp.status_code != 201:
            print("FAILED: Creation failed")
            return
            
        data = resp.json()
        doc_id = data.get("id")
        print(f"Created ID: {doc_id}")
    except Exception as e:
        print(f"FAILED: Connection error - {e}")
        return

    # 3. Test List (GET)
    print(f"\n2. Testing GET /{tenant_id}/{resource}...")
    try:
        resp = requests.get(f"{BASE_URL}/{tenant_id}/{resource}")
        print(f"Status: {resp.status_code}")
        data = resp.json()
        print(f"Count: {len(data.get('data', []))}")
        
        found = False
        for item in data.get("data", []):
            if item.get("id") == doc_id:
                print("SUCCESS: Created item found in list")
                found = True
                break
        
        if not found:
            print("FAILED: Created item NOT found in list")
    except Exception as e:
         print(f"FAILED: Connection error - {e}")

    # 4. Test Get One (GET ID)
    print(f"\n3. Testing GET /{tenant_id}/{resource}/{doc_id}...")
    try:
        resp = requests.get(f"{BASE_URL}/{tenant_id}/{resource}/{doc_id}")
        print(f"Status: {resp.status_code}")
        item = resp.json()
        
        if item.get("email") == "rest.api@example.com":
             print("SUCCESS: Fetched item matches")
        else:
             print(f"FAILED: Item content mismatch: {item}")
             
    except Exception as e:
         print(f"FAILED: Connection error - {e}")

    # 5. Test Filtering
    print(f"\n4. Testing GET /{tenant_id}/{resource}?email=...")
    try:
        resp = requests.get(f"{BASE_URL}/{tenant_id}/{resource}", params={"email": "rest.api@example.com"})
        data = resp.json()
        count = len(data.get("data", []))
        print(f"Filtered Count: {count}")
        if count >= 1:
            print("SUCCESS: Filtering worked")
        else:
            print("FAILED: Filtering returned no results")
    except Exception as e:
         print(f"FAILED: Connection error - {e}")

if __name__ == "__main__":
    run_test()
