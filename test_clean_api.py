
import requests
import json

def test_api():
    url = "http://localhost:5000/api/import/ai/clean"
    payload = {
        "tenantId": "acme-corp",
        "templateKey": "People",
        "rows": [
            {"__rowIndex": 0, "type": "Candidate", "id": "123", "title": "Mr", "firstName": "John", "surname": "Doe", "gender": "M", "dob": "01-01-1990", "email": "john@example.com"}
        ],
        "useAi": False
    }
    try:
        res = requests.post(url, json=payload)
        print(f"Status: {res.status_code}")
        print(f"Response: {res.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_api()
