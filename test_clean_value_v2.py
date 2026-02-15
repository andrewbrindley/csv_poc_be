
import sys
import unittest
from unittest.mock import MagicMock

# Mock dependencies to load main.py
sys.modules["db_config"] = MagicMock()
sys.modules["processing_worker"] = MagicMock()
sys.modules["template_api"] = MagicMock()
# main imports core_config? No, it defines things inline or imports standard libs
# It imports `from dotenv import load_dotenv` which is fine.

import main

class TestCleanValue(unittest.TestCase):
    def test_clean_value_email(self):
        field = {
            "key": "email",
            "label": "Email",
            "pattern": "email",
            "blockFreeEmail": True,
            "domainWhitelist": "acme.com, example.org",
            "required": True
        }
        
        # Valid
        val, status = main.clean_value("People", field, "john@acme.com")
        self.assertEqual(status, "ok", "Should accept valid whitelisted email")
        
        # Invalid (Free provider)
        val, status = main.clean_value("People", field, "john@gmail.com")
        self.assertEqual(status, "bad", "Should block gmail.com")

        # Invalid (Not whitelisted)
        val, status = main.clean_value("People", field, "john@other.com")
        self.assertEqual(status, "bad", "Should block non-whitelisted domain")

    def test_clean_value_enum(self):
        field = {
            "key": "status",
            "label": "Status",
            "pattern": "enum",
            "allowed": ["Active", "Inactive"],
            "caseSensitive": False
        }
        
        # Valid Exact
        val, status = main.clean_value("Any", field, "Active")
        self.assertEqual(status, "ok")
        
        # Valid Case Insensitive
        val, status = main.clean_value("Any", field, "active")
        self.assertEqual(status, "ok")
        self.assertEqual(val, "Active", "Should normalize case")

        # Invalid
        val, status = main.clean_value("Any", field, "Unknown")
        self.assertEqual(status, "bad")

    def test_clean_value_crashes(self):
        # Test generic safe handling
        field = {"key": "test", "pattern": "string"}
        try:
            main.clean_value("Any", field, None)
            main.clean_value("Any", field, 123)
            main.clean_value("Any", field, "Safe")
        except Exception as e:
            self.fail(f"clean_value crashed on basic input: {e}")

if __name__ == "__main__":
    unittest.main()
