import requests
try:
    r = requests.get("http://localhost:5000/api/templates?tenantId=acme-corp")
    data = r.json()
    print("Keys found in API:", [t.get("templateKey") for t in data.get("templates", [])])
except Exception as e:
    print(e)
