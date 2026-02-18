
from db_config import get_templates_collection

def check_templates():
    coll = get_templates_collection()
    if coll is None:
        print("DB Not Connected")
        return

    tenant_id = "acme-corp"
    templates = list(coll.find({"tenantId": tenant_id}))
    
    print(f"Found {len(templates)} templates for {tenant_id}:")
    for t in templates:
        print(f" - Key: {t.get('templateKey')}, Label: {t.get('templateLabel')}")

if __name__ == "__main__":
    check_templates()
