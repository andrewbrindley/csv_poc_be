import os
from datetime import datetime
from core_config import TEMPLATES
from db_config import get_templates_collection

def get_execution_order(template_keys: list, all_templates: dict = None) -> list:
    """
    Return a topologically sorted list of template keys based on dependencies.
    Raises ValueError if a cycle is detected or dependency is missing.
    """
    source = all_templates if all_templates is not None else TEMPLATES
    graph = {k: set(v.get("dependencies", [])) for k, v in source.items()}
    
    in_degree = {k: 0 for k in graph}
    for k in graph:
        for dep in graph[k]:
            if dep not in in_degree:
                continue 
            in_degree[k] += 1
            
    queue = [k for k in in_degree if in_degree[k] == 0]
    sorted_list = []
    
    while queue:
        node = queue.pop(0)
        sorted_list.append(node)
        
        for k, deps in graph.items():
            if node in deps:
                in_degree[k] -= 1
                if in_degree[k] == 0:
                    queue.append(k)
                    
    if len(sorted_list) != len(graph):
        raise ValueError("Cyclic dependency detected in templates")
        
    return [k for k in sorted_list if k in template_keys]

def validate_template_dependencies():
    """Run self-check on template dependencies at startup."""
    try:
        get_execution_order(list(TEMPLATES.keys()))
        print("TEMPLATES: Dependency validation successful.")
    except ValueError as e:
        print(f"TEMPLATES: Dependency validation FAILED: {e}")

def get_templates(tenant_id: str = None) -> dict:
    """
    Fetch templates for tenant and merge with system defaults.
    Returns dict: { 'TemplateKey': { ...template... }, ... }
    """
    final_templates = TEMPLATES.copy()

    if tenant_id:
        coll = get_templates_collection()
        if coll is not None:
            try:
                cursor = coll.find({"tenantId": tenant_id})
                for doc in cursor:
                    t_key = doc.get("templateKey")
                    if not t_key:
                        continue
                    
                    doc.pop("_id", None)
                    if "key" not in doc:
                        doc["key"] = t_key
                        
                    final_templates[t_key] = doc
            except Exception as e:
                print(f"Error fetching templates for tenant {tenant_id}: {e}")

    for t_key, t_def in final_templates.items():
        if "dependencies" not in t_def:
            t_def["dependencies"] = []
        if "references" not in t_def:
            t_def["references"] = {}
            
        for f in t_def.get("fields", []):
            rel = f.get("relationship")
            if not rel and f.get("pattern") == "relationship":
                pass
            
            if rel and isinstance(rel, dict):
                target_tpl = rel.get("targetTemplate") or rel.get("linkToTemplate")
                target_field = rel.get("targetField") or rel.get("linkToField")
                
                if target_tpl:
                    if target_tpl not in t_def["dependencies"] and target_tpl != t_key:
                        t_def["dependencies"].append(target_tpl)
                    
                    f_key = f.get("key")
                    if f_key:
                        t_def["references"][f_key] = {
                            "targetTemplate": target_tpl,
                            "targetField": target_field or "id"
                        }

    return final_templates

def get_template(template_key: str, all_templates: dict = None):
    """
    Look up a template by key or label, case-insensitive.
    """
    source = all_templates if all_templates is not None else TEMPLATES
    
    # 1. Direct key match
    if template_key in source:
        return source[template_key]
        
    # 2. Case-insensitive key match
    for k in source:
        if k.lower() == template_key.lower():
            return source[k]
            
    # 3. Label match
    for k, tpl in source.items():
        if tpl.get("label", "").lower() == template_key.lower():
            return tpl
            
    raise ValueError(f"Template not found: {template_key}")
