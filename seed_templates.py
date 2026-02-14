"""
Seed script to migrate default templates to the database.

This script copies the hardcoded TEMPLATES from main.py into the MongoDB
templates collection for a specific tenant.

Usage:
    python seed_templates.py <tenantId>

Example:
    python seed_templates.py acme-corp
"""

import sys
from datetime import datetime
from db_config import get_templates_collection
from main import TEMPLATES

def seed_templates(tenant_id):
    """Seed default templates into the database for a tenant."""
    coll_templates = get_templates_collection()
    if coll_templates is None:
        print("ERROR: MongoDB not connected")
        return False
    
    print(f"Seeding templates for tenant: {tenant_id}")
    print(f"Found {len(TEMPLATES)} default templates to seed")
    
    seeded_count = 0
    skipped_count = 0
    
    for template_key, template_data in TEMPLATES.items():
        # Check if template already exists
        existing = coll_templates.find_one({
            "tenantId": tenant_id,
            "templateKey": template_key
        })
        
        if existing:
            print(f"  ⏭️  Skipping '{template_key}' (already exists)")
            skipped_count += 1
            continue
        
        # Create template document
        template_doc = {
            "tenantId": tenant_id,
            "templateKey": template_data["key"],
            "templateLabel": template_data["label"],
            "keywords": template_data.get("keywords", []),
            "dependencies": template_data.get("dependencies", []),
            "references": template_data.get("references", {}),
            "fields": template_data["fields"],
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
            "createdBy": "seed_script"
        }
        
        result = coll_templates.insert_one(template_doc)
        print(f"  ✅ Seeded '{template_key}' (ID: {result.inserted_id})")
        seeded_count += 1
    
    print(f"\n✨ Done! Seeded {seeded_count} templates, skipped {skipped_count}")
    return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python seed_templates.py <tenantId>")
        print("Example: python seed_templates.py acme-corp")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    success = seed_templates(tenant_id)
    sys.exit(0 if success else 1)
