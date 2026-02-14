
import requests
import json

URL = "http://localhost:5000/graphql"
TENANT_ID = "test_tenant_api_verify" # Using same tenant as REST test

def test_graphql():
    print("--- TESTING GRAPHQL ---")
    
    # 1. Create Data if not exists (using REST API for setup?)
    # We assume 'People' exist from previous verification
    
    # 2. Define Query
    # Fetch People and their Nested Bookings
    query = """
    query {
      people(limit: 5) {
        mongoId
        id
        firstName
        surname
        bookings {
          assessmentDate
          status
        }
      }
    }
    """
    
    print(f"Query: {query}")
    
    try:
        # Note: We must pass tenantId as query param for context injection
        resp = requests.post(f"{URL}?tenantId={TENANT_ID}", json={"query": query})
        
        print(f"Status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"Error: {resp.text}")
            return

        data = resp.json()
        print("Response JSON:")
        print(json.dumps(data, indent=2))
        
        # Validation
        if "data" in data and "people" in data["data"]:
            people = data["data"]["people"]
            print(f"Found {len(people)} people")
            if len(people) > 0:
                print("SUCCESS: GraphQL Query execute correctly")
        else:
            print("FAILED: Unexpected response structure")

    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    test_graphql()
