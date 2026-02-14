import requests
import json

BASE_URL = "http://localhost:5000/api"

def get_tenants():
    try:
        response = requests.get(f"{BASE_URL}/tenants")
        response.raise_for_status()
        data = response.json()
        print("Tenants:", json.dumps(data, indent=2))
        return data.get("tenants", [])
    except Exception as e:
        print(f"Error fetching tenants: {e}")
        return []

def get_search_data(tenant_id):
    # Test all core templates
    templates = ["People", "Bookings", "PatientData"]
    
    for resource in templates:
        url = f"{BASE_URL}/v1/{tenant_id}/{resource}?limit=5"
        try:
            response = requests.get(url)
            if response.status_code == 404:
                print(f"Resource {resource} not found for {tenant_id}")
                continue
            response.raise_for_status()
            data = response.json()
            total = data.get("meta", {}).get("total", 0)
            print(f"Data for {tenant_id}/{resource}: Found {total} records")
            if total > 0:
                 print(f"WARNING: Found {total} records in {resource} after reset!")
        except Exception as e:
            print(f"Error fetching search data for {resource}: {e}")

if __name__ == "__main__":
    tenants = get_tenants()
    if not tenants:
        print("No tenants found or error occurred.")
        # Fallback to hardcoded commonly used tenant if list fails
        tenants = [{"id": "acme-corp", "name": "Acme Corp"}]
    
    for tenant in tenants:
        tenant_id = tenant.get("id")
        print(f"\n--- Testing Tenant: {tenant_id} ---")
        get_search_data(tenant_id)
