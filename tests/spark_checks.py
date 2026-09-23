"""Real Spark checks for the transformations; run separately from stdlib tests."""
import sys
import unittest
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipelines"))
from framework import load_contracts, normalize, rag_projection, raw_schema, validity_expression


class SparkChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("insurance-contract-tests").config("spark.sql.session.timeZone", "UTC").getOrCreate()
        cls.config = load_contracts()[0]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, **changes):
        row = dict(claim_id="CLM-001", policy_id="POL-001", product="motor", status="open", loss_type="collision", amount="12.50", currency="INR", updated_at="2026-09-23T10:00:00Z", _rescued_data=None, _corrupt_record=None)
        row.update(changes)
        raw = self.spark.createDataFrame([row], schema=raw_schema(self.config)).withColumn("_source_file", F.lit("s3://approved/claims/1.json"))
        return normalize(raw, self.config)

    def test_valid_record_is_typed_and_projected(self):
        frame = self.frame()
        self.assertEqual(frame.where(validity_expression(self.config)).count(), 1)
        projected = rag_projection(frame, self.config)
        self.assertNotIn("_source_file", projected.columns)
        self.assertNotIn("_rescued_data", projected.columns)
        self.assertIn("12.50 INR", projected.first().content)

    def test_invalid_records_go_only_to_quarantine(self):
        for changes in ({"amount": "invalid"}, {"amount": "-1"}, {"amount": None}, {"updated_at": "bad"}, {"updated_at": "2026-09-23T10:00:00"}, {"product": "private text"}, {"_rescued_data": '{"email":"x"}'}, {"claim_id": None}):
            with self.subTest(changes=changes):
                frame = self.frame(**changes)
                condition = validity_expression(self.config)
                self.assertEqual(frame.where(condition).count(), 0)
                self.assertEqual(frame.where(f"NOT ({condition})").count(), 1)

    def test_payload_hash_is_stable(self):
        self.assertEqual(self.frame().first().row_hash_key, self.frame().first().row_hash_key)

    def test_equivalent_timezones_order_equally(self):
        self.assertEqual(self.frame().first().updated_at, self.frame(updated_at="2026-09-23T15:30:00+05:30").first().updated_at)


if __name__ == "__main__":
    unittest.main()
