import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipelines"))
from framework import load_contracts, quality_rules, raw_schema


class ContractTests(unittest.TestCase):
    def check_invalid(self, change):
        config = copy.deepcopy(load_contracts()[0])
        change(config)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "datasets.json"
            path.write_text(json.dumps({"datasets": [config]}))
            with self.assertRaises(ValueError):
                load_contracts(path)

    def test_contract_is_valid(self):
        self.assertEqual(load_contracts()[0]["name"], "claims")

    def test_path_traversal_rejected(self):
        self.check_invalid(lambda c: c.update(source_subpath="../../other"))

    def test_unapproved_column_rejected(self):
        self.check_invalid(lambda c: c["fields"].update(email="string"))

    def test_missing_required_field_rejected(self):
        self.check_invalid(lambda c: c["required"].remove("policy_id"))

    def test_metadata_cannot_inject_sql(self):
        self.check_invalid(lambda c: c.update(products=["motor'); DROP TABLE claims; --"]))

    def test_bad_keys_rejected(self):
        self.check_invalid(lambda c: c.update(keys=["policy_id"]))

    def test_schema_drift_and_bad_records_have_rules(self):
        config = load_contracts()[0]
        self.assertIn("_corrupt_record", raw_schema(config))
        self.assertIn("clean_schema", quality_rules(config))
