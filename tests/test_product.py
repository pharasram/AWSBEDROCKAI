import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from insurance_rag.core import LocalStore, document, stage, validate
from insurance_rag.cloud import Bedrock

ROOT = Path(__file__).resolve().parents[1]
ROWS = json.loads((ROOT / "data/synthetic_claims.json").read_text())


class ProductTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(Path(self.tmp.name) / "test.db")

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_count_mismatch_is_atomic(self):
        with self.assertRaises(ValueError):
            self.store.ingest(ROWS, 99, "bad")
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM docs").fetchone()[0], 0)

    def test_schema_rejects_unknown_pii(self):
        rows = copy.deepcopy(ROWS)
        rows[0]["email"] = "private@example.test"
        with self.assertRaises(ValueError):
            validate(rows, 6)

    def test_invalid_amounts(self):
        for amount in (-1, True, float("nan"), float("inf"), "12"):
            row = {**ROWS[0], "amount": amount}
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                validate([row], 1)

    def test_timezone_required(self):
        with self.assertRaises(ValueError):
            validate([{**ROWS[0], "updated_at": "2026-09-01T10:00:00"}], 1)

    def test_dedup_selects_latest(self):
        rows = stage(ROWS, "b1")
        self.assertEqual(len(rows), 5)
        self.assertEqual(next(r for r in rows if r["claim_id"] == "CLM-1001")["status"], "review")

    def test_reingestion_is_idempotent(self):
        for _ in range(2):
            self.store.ingest(ROWS, 6, "batch1")
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM docs").fetchone()[0], 5)

    def test_out_of_order_batch_does_not_revert(self):
        self.store.ingest(ROWS, 6, "latest")
        self.store.ingest([ROWS[0]], 1, "older")
        payload = json.loads(self.store.db.execute("SELECT payload FROM docs WHERE id='CLM-1001'").fetchone()[0])
        self.assertEqual(payload["status"], "review")

    def test_hash_stable(self):
        self.assertEqual(stage(ROWS, "a")[0]["row_hash_key"], stage(ROWS, "b")[0]["row_hash_key"])

    def test_projection_excludes_extra_data(self):
        self.assertNotIn("private", document({**ROWS[0], "name": "private"}))

    def test_retrieval_has_citations(self):
        self.store.ingest(ROWS, 6, "b")
        answer = self.store.query("flood")
        self.assertEqual(answer["sources"][0]["source_id"], "CLM-1002")
        self.assertIn("[CLM-1002]", answer["answer"])

    def test_scope_filter(self):
        self.store.ingest(ROWS, 6, "b")
        self.assertEqual(self.store.query("flood", product="motor")["sources"], [])

    def test_no_evidence(self):
        self.assertIn("Insufficient", self.store.query("quantum reactor")["answer"])

    def test_question_validation(self):
        for question in ("", " " * 10, "a" * 2001, None):
            with self.subTest(question=str(question)[:10]), self.assertRaises(ValueError):
                self.store.query(question)

    def test_bedrock_embedding_contract(self):
        client = Mock()
        client.invoke_model.return_value = {"body": io.BytesIO(b'{"embedding":[0.1,0.2]}')}
        self.assertEqual(Bedrock(client).embed("example"), [0.1, 0.2])
        args = client.invoke_model.call_args.kwargs
        self.assertEqual(json.loads(args["body"])["dimensions"], 1024)
        self.assertEqual(args["modelId"], "amazon.titan-embed-text-v2:0")

    def test_empty_evidence_skips_model(self):
        client = Mock()
        Bedrock(client).answer("unknown", [])
        client.converse.assert_not_called()


class TriggerTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("trigger", ROOT / "cloud/handler.py")
        self.handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.handler)
        self.event = {"detail": {"bucket": {"name": "stage"}, "object": {"key": "events/stage/b.json", "version-id": "v1"}}}
        self.env = patch.dict(os.environ, {"STAGE_BUCKET": "stage", "AUDIT_TABLE": "audit", "DATABRICKS_JOB_ID": "1"})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.aws = Mock()
        self.modules = patch.dict("sys.modules", {"boto3": self.aws})
        self.modules.start()
        self.addCleanup(self.modules.stop)

    def test_duplicate_events_share_token(self):
        with patch.object(self.handler, "databricks", return_value={"run_id": 7}) as api:
            a = self.handler.main(self.event, None)
            b = self.handler.main(self.event, None)
            self.assertEqual(a["batch_id"], b["batch_id"])
            self.assertEqual(len(api.call_args.args[1]["idempotency_token"]), 64)

    def test_wrong_bucket_rejected(self):
        self.event["detail"]["bucket"]["name"] = "wrong"
        with self.assertRaises(ValueError):
            self.handler.main(self.event, None)

    def test_failed_run_is_terminal(self):
        with patch.object(self.handler, "databricks", return_value={"state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}}):
            self.assertEqual(self.handler.main({"action": "poll", "batch_id": "b", "run_id": 7}, None)["status"], "FAILED")

    def test_success_is_terminal(self):
        with patch.object(self.handler, "databricks", return_value={"state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}}):
            self.assertEqual(self.handler.main({"action": "poll", "batch_id": "b", "run_id": 7}, None)["status"], "SUCCEEDED")

    def test_running_remains_running(self):
        with patch.object(self.handler, "databricks", return_value={"state": {"life_cycle_state": "RUNNING"}}):
            self.assertEqual(self.handler.main({"action": "poll", "batch_id": "b", "run_id": 7}, None)["status"], "RUNNING")


if __name__ == "__main__":
    unittest.main()
