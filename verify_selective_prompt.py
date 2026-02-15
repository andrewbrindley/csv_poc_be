
import json
from main import build_clean_prompt, TEMPLATES

def test_selective_prompt():
    template_key = "People"
    tpl = TEMPLATES[template_key]
    rows = [{"__rowIndex": 0, "firstName": "john", "surname": "SMITH", "type": "InvalidType"}]
    row_meta = [{"__rowIndex": 0, "fields": {
        "firstName": {"isValid": True, "isRequired": True},
        "surname": {"isValid": True, "isRequired": True},
        "type": {"isValid": False, "isRequired": True}
    }}]
    
    # Test setting where onlyFixInvalid is TRUE
    settings = {"onlyFixInvalid": True}
    prompt_str = build_clean_prompt(template_key, tpl, rows, row_meta, settings=settings)
    prompt = json.loads(prompt_str)
    
    print("--- SELECTIVE CLEANING PROMPT ---")
    print(f"Goal: {prompt['instructions']['goal']}")
    print(f"Strict Rule: {prompt['instructions']['rules'][0]}")
    
    # Assertions
    assert "Repair ONLY the reported errors" in prompt['instructions']['goal']
    assert "CRITICAL: The 'Clean Valid Values' flag is DISABLED" in prompt['instructions']['rules'][0]
    
    # Test setting where onlyFixInvalid is FALSE
    settings_off = {"onlyFixInvalid": False}
    prompt_str_off = build_clean_prompt(template_key, tpl, rows, row_meta, settings=settings_off)
    prompt_off = json.loads(prompt_str_off)
    
    print("\n--- NORMAL CLEANING PROMPT ---")
    print(f"Goal: {prompt_off['instructions']['goal']}")
    assert "Clean and normalise CSV rows" in prompt_off['instructions']['goal']
    
    print("\nVerification Successful!")

if __name__ == "__main__":
    try:
        test_selective_prompt()
    except Exception as e:
        print(f"Verification Failed: {e}")
