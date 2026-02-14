
import requests
import json

url = "http://localhost:5000/api/tenant-data/search"
params = {
    "tenantId": "acme-corp",
    "templateKey": "People",
    "limit": 10
}


try:
    with open("debug_res.txt", "w", encoding="utf-8") as f:
        f.write(f"Requesting {url} with params {params}\n")
        resp = requests.get(url, params=params)
        f.write(f"Status: {resp.status_code}\n")
        f.write(f"Content: {resp.text[:500]}...\n") 
        try:
            data = resp.json()
            f.write(f"JSON Data Count: {len(data.get('data', []))}\n")
            if len(data.get('data', [])) > 0:
                f.write(f"First Item: {data['data'][0]}\n")
        except:
            f.write("Failed to parse JSON\n")
except Exception as e:
    with open("debug_res.txt", "w", encoding="utf-8") as f:
        f.write(f"Error: {e}\n")
