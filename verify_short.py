
import requests, sys
try:
    current = requests.get("http://localhost:5000/api/templates?tenantId=acme-corp").json().get("templates", [])
    has_sys = any(t.get('key')=='People' for t in current)
    has_cust = any("_id" in t for t in current)
    print(f"SYS={has_sys}, CUST={has_cust}")
except Exception as e:
    print(e)
