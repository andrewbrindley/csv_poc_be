
import graphene
from graphene import ObjectType, String, Int, List, Field, ID, Boolean, JSONString
from db_config import get_tenant_data_collection
from datetime import datetime
import re

# Cache for generated types to handle circular references
TYPE_CACHE = {}

def clean_key(k):
    """Ensure key is GraphQL compatible (alphanumeric)."""
    # Replace spaces with underscores, remove other non-alphanumeric
    if not k:
        return "Unknown"
    s = re.sub(r'[^a-zA-Z0-9_]', '', str(k))
    if not s:
        return "Unknown"
    # Ensure starts with letter
    if s[0].isdigit():
        s = "Type" + s
    return s

def process_doc(doc):
    """Flatten Mongo doc for Graphene."""
    d = doc.get("data", {}).copy() # Avoid mutating original
    
    # Inject metadata fields
    d['mongoId'] = str(doc.get('_id'))
    
    # Handle dates
    ts = doc.get('timestamp')
    if isinstance(ts, datetime):
         d['createdAt'] = ts.isoformat()
    else:
         d['createdAt'] = str(ts)
         
    d['jobId'] = doc.get('jobId')
    d['rawData'] = doc.get('data') # For the JSONString field
    
    return d

def create_dynamic_type(template_key, template_def):
    """
    Dynamically create a Graphene ObjectType for a given template.
    """
    if template_key in TYPE_CACHE:
        return TYPE_CACHE[template_key]

    type_name = f"{clean_key(template_key)}Type"

    # 1. Define Basic Fields
    fields = {
        "mongoId": ID(description="The unique ID of the record (Mongo _id)"),
        "rawData": JSONString(description="The full raw data object"),
        "createdAt": String(),
        "jobId": String(),
    }

    # Map template fields to GraphQL types
    for field_def in template_def.get("fields", []):
        f_key = field_def.get("key")
        if not f_key: continue
        
        # Use label for the GraphQL key so custom fields are readable
        label = field_def.get("label") or f_key
        
        # Convert label to camelCase equivalent (e.g. "NoteID" -> "noteid")
        gql_key = clean_key(label)
        if gql_key:
            gql_key = gql_key[0].lower() + gql_key[1:]
        
        # We need a closure to capture the internal f_key
        def make_field_resolver(internal_key):
            def resolver(root, info, **kwargs):
                return root.get(internal_key)
            return resolver
            
        # Determine Graphene Type
        f_type = String(resolver=make_field_resolver(f_key)) # Default
        pat = field_def.get("pattern")
        if pat == "integer":
            f_type = Int(resolver=make_field_resolver(f_key))
        elif pat == "boolean":
             f_type = Boolean(resolver=make_field_resolver(f_key))
            
        fields[gql_key] = f_type

    # 2. Create the Type Class using type()
    # Graphene automatically resolves fields from the root dict if keys match.
    # Our `process_doc` flattens data so keys line up.
    
    DynType = type(type_name, (ObjectType,), fields)
    TYPE_CACHE[template_key] = DynType
    return DynType

def wire_relationships(templates_dict):
    """
    Run AFTER all types are created to wire up 'references'.
    Add fields dynamically to existing classes.
    """
    for t_key, t_def in templates_dict.items():
        # Source Type: The Parent (e.g. People)
        # We want to find Children (e.g. Bookings) that point to us.
        
        # Iterate all templates to find back-references (Incoming Edges)
        for child_key, child_def in templates_dict.items():
            if child_key == t_key: continue
            
            for field in child_def.get("fields", []):
                if field.get("pattern") in ("relationship", "reference"):
                    # Check both nested relationship config (core defaults) or flat config (UI custom templates)
                    ref_conf = field.get("relationship", field)
                    if ref_conf.get("targetTemplate") == t_key:
                        # Found a relationship!
                        # Child (Bookings) -> Parent (People) via child_field_ref (personNumber)
                        # We want to add field `bookings` to `PersonType`.
                        
                        child_field_ref = field.get("key")
                        
                        parent_type = TYPE_CACHE.get(t_key)
                        child_type = TYPE_CACHE.get(child_key)
                        
                        if not parent_type or not child_type:
                            continue
                            
                        # Field name: "bookings"
                        rel_name = clean_key(child_key).lower()
                        
                        # Store variables for closure
                        target_field_on_parent = ref_conf.get("targetField") # e.g. "id" (Candidate No)
                        c_key = child_key
                        c_field = child_field_ref
                        
                        def make_resolver(child_k, child_f, parent_f):
                            def resolver(root, info, limit=10, **kwargs):
                                # root is the Parent (Person dict)
                                # Get the value of the joining key from parent
                                # root is flattened, so 'id' (CandidateNo) is at root['id'] (if mapped) or root[parent_f]
                                
                                parent_val = root.get(parent_f)
                                if not parent_val:
                                    # Fallback: check if parent_f is 'id' but mapped to '_id'?
                                    # Usually 'id' in root is MongoID. 'id' in data is CandidateNo.
                                    # Our process_doc keeps root[parent_f] if it was in data.
                                    pass
                                    
                                if not parent_val:
                                    return []
                                    
                                coll = get_tenant_data_collection()
                                if coll is None: return []
                                
                                # Query the *Child* collection
                                # "data.personNumber": "1001"
                                # tenantId from context
                                t_id = info.context.get("tenant_id")
                                
                                query = {
                                    "templateKey": child_k,
                                    "tenantId": t_id,
                                    f"data.{child_f}": parent_val
                                }
                                
                                # print(f"GRAPHQL REL: {query}")
                                cursor = coll.find(query).limit(limit)
                                return [process_doc(d) for d in cursor]
                            return resolver

                        # Add the list field to the Parent class
                        # Graphene allows adding fields to _meta.fields but sticking it as attribute + resolver method works too if done right.
                        # Best way for runtime mutation is strictly defining the class, but we already created it.
                        # Graphene classes are complex metaclasses. 
                        # Simpler hack: We rebuild the class? No, circular dependency.
                        # Proper way: mount resolver as method `resolve_field`.
                        
                        # We can use `setattr` on the class for the resolver method
                        resolver_name = f"resolve_{rel_name}"
                        setattr(parent_type, resolver_name, make_resolver(c_key, c_field, target_field_on_parent))
                        
                        # key in _meta.fields
                        parent_type._meta.fields[rel_name] = Field(List(child_type), limit=Int(default_value=10))


def build_schema(templates_dict):
    # 1. Clear Cache
    TYPE_CACHE.clear()
    
    # 2. Create Types (Nodes)
    for t_key, t_def in templates_dict.items():
        create_dynamic_type(t_key, t_def)
        
    # 3. Wire Relationships (Edges)
    wire_relationships(templates_dict)
    
    # 4. Create Root Query
    fields = {}
    
    for t_key in templates_dict.keys():
        t_type = TYPE_CACHE.get(t_key)
        if not t_type: continue
        
        # Field name: "people", "bookings"
        field_name = clean_key(t_key).lower()
        
        # Resolver
        def make_root_resolver(tk):
            def resolver(root, info, limit=50, skip=0, **kwargs):
                t_id = info.context.get("tenant_id")
                coll = get_tenant_data_collection()
                if coll is None or not t_id: return []
                
                query = {"tenantId": t_id, "templateKey": tk}
                
                # Filters
                # We could support generic kwargs mapping to data.*
                for k, v in kwargs.items():
                     if k not in ['limit', 'skip']:
                         query[f"data.{k}"] = v
                
                cursor = coll.find(query).skip(skip).limit(limit)
                return [process_doc(d) for d in cursor]
            return resolver
            
        fields[field_name] = List(t_type, limit=Int(default_value=50), skip=Int(default_value=0))
        fields[f"resolve_{field_name}"] = make_root_resolver(t_key)

    Query = type("Query", (ObjectType,), fields)
    
    # If no fields were added, add a dummy field to prevent schema errors
    if not fields:
        Query = type("Query", (ObjectType,), {"_empty": String(resolver=lambda *_: "No templates found")})
        
    return graphene.Schema(query=Query)
