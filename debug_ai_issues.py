
import json
from main import (
    clean_value, 
    clean_rows_for_template, 
    build_row_meta_for_prompt, 
    TEMPLATES, 
    build_clean_prompt
)

def debug_cleaning_issues():
    print("--- 1. Debugging '???' in Location ---")
    template_key = "Bookings"
    tpl = TEMPLATES[template_key]
    
    # Simulate a row with "???" in locationName
    row_input = {"locationName": "???"}
    field_def = next(f for f in tpl["fields"] if f["key"] == "locationName")
    
    # Manual check of clean_value
    val, status = clean_value(template_key, field_def, "???")
    print(f"clean_value output for '???': ('{val}', '{status}')")
    
    # Check full row cleaning
    rows = [{"__rowIndex": 0, "locationName": "???", "bookingRef": "123", "personNumber": "100", "assessmentName": "Test", "assessmentDate": "01-01-2025"}]
    cleaned, row_errors, _ = clean_rows_for_template(template_key, rows)
    
    print(f"Row Errors for index 0: {row_errors.get(0)}")
    
    # Build meta
    meta = build_row_meta_for_prompt(template_key, tpl, cleaned, row_errors)
    location_meta = meta[0]["fields"]["locationName"]
    print(f"Meta for locationName: {location_meta}")
    
    # Check Prompt Generation (Strict Mode)
    settings = {"onlyFixInvalid": True, "ensureNoEmptyValues": True}
    prompt_str = build_clean_prompt(template_key, tpl, cleaned, meta, settings=settings)
    prompt = json.loads(prompt_str)
    
    # Verifying Strict Instruction
    strict_rule = prompt["instructions"]["rules"][0]
    print(f"First Rule in Prompt: {strict_rule[:100]}...")
    
    
    print("\n--- 2. Debugging Empty IDs ---")
    # Simulate empty personNumber
    row_empty_id = [{"__rowIndex": 1, "personNumber": "", "bookingRef": "124", "assessmentName": "Test", "assessmentDate": "01-01-2025"}]
    cleaned_id, errors_id, _ = clean_rows_for_template(template_key, row_empty_id)
    
    print(f"Row Errors for index 1 (empty ID): {errors_id.get(1)}")
    
    # Check if ensureNoEmptyValues is mentioned in PII instruction
    pii_rule = next(r for r in prompt["instructions"]["rules"] if r.startswith("PII:"))
    print(f"PII Rule: {pii_rule}")

if __name__ == "__main__":
    debug_cleaning_issues()
