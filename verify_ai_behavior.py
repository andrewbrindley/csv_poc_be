
import json
from main import build_clean_prompt, TEMPLATES

def verify_ai_behavior():
    print("--- Verifying AI Prompt Logic ---")
    template_key = "Bookings"
    tpl = TEMPLATES[template_key]
    rows = [{"__rowIndex": 0, "locationName": "???", "personNumber": ""}]
    row_meta = [{"__rowIndex": 0, "fields": {
        "locationName": {"isValid": True, "isRequired": True},
        "personNumber": {"isValid": False, "isRequired": True}
    }}]

    # Case 1: Strict Selective Cleaning + Ensure No Empty Values
    settings = {"onlyFixInvalid": True, "ensureNoEmptyValues": True}
    prompt_str = build_clean_prompt(template_key, tpl, rows, row_meta, settings=settings)
    prompt = json.loads(prompt_str)
    
    rules = prompt["instructions"]["rules"]
    print(f"\n[Case 1] Rules (Selective + EnsureEmpty):")
    for r in rules:
        if r.startswith("CRITICAL"):
            print(f"  - SELECTIVE: {r[:60]}...")
            assert "even if it contains '???', 'Unknown'" in r
        if r.startswith("PII & IDs"):
            print(f"  - PII: {r[:60]}...")
            assert "MUST fill ALL missing IDs" in r
        if r.startswith("Strictness"):
            print(f"  - STRICTNESS: {r[:60]}...")
            assert "REQUIRED to invent/hallucinate" in r

    # Case 2: Normal + No Ensure Empty
    settings_normal = {"onlyFixInvalid": False, "ensureNoEmptyValues": False}
    prompt_str_normal = build_clean_prompt(template_key, tpl, rows, row_meta, settings=settings_normal)
    prompt_normal = json.loads(prompt_str_normal)
    
    rules_normal = prompt_normal["instructions"]["rules"]
    print(f"\n[Case 2] Rules (Normal):")
    for r in rules_normal:
        if r.startswith("PII"):
            print(f"  - PII: {r}")
            assert "do not invent them" in r
        if r.startswith("Strictness"):
            print(f"  - STRICTNESS: {r}")
            assert "Only hallucinate synthetic values if settings.ensureNoEmptyValues is true" in r

    print("\nVerification Successful: Prompt logic adapts correctly to settings.")

if __name__ == "__main__":
    try:
        verify_ai_behavior()
    except Exception as e:
        print(f"Verification Failed: {e}")
        exit(1)
