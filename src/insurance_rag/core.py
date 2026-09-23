"""Deterministic contracts, safe document projection, and local retrieval."""
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone

FIELDS = {"claim_id", "policy_id", "product", "status", "loss_type", "amount", "currency", "updated_at"}
STOP = {"a", "an", "the", "is", "are", "what", "which", "show", "me", "of", "for", "in", "and", "to", "claims", "claim"}


def validate(rows, expected_count):
    if len(rows) != expected_count:
        raise ValueError(f"Count mismatch: expected {expected_count}, received {len(rows)}")
    for row in rows:
        if set(row) != FIELDS:
            raise ValueError("Schema mismatch: expected the approved insurance contract")
        if any(not isinstance(row[k], str) or not row[k].strip() for k in FIELDS - {"amount"}):
            raise ValueError("Required string is empty or invalid")
        if isinstance(row["amount"], bool) or not isinstance(row["amount"], (int, float)):
            raise ValueError("Amount must be numeric")
        import math
        if not math.isfinite(row["amount"]) or row["amount"] < 0:
            raise ValueError("Amount must be finite and non-negative")
        stamp = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("updated_at must include a timezone")


def stage(rows, batch_id):
    validate(rows, len(rows))
    newest = {}
    for row in rows:
        key = row["claim_id"]
        # Stable tie-breaker makes reruns independent of source ordering.
        rank = (datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00")), json.dumps(row, sort_keys=True))
        if key not in newest or rank > newest[key][0]:
            newest[key] = (rank, row)
    result = []
    for _, row in newest.values():
        canonical = json.dumps(row, sort_keys=True, separators=(",", ":"))
        result.append({**row, "row_hash_key": hashlib.sha256(canonical.encode()).hexdigest(),
                       "transaction_correlation_id": batch_id, "source_code": "insurance",
                       "load": "upsert", "stage_load_ts": datetime.now(timezone.utc).isoformat()})
    return result


def document(row):
    # Explicit projection prevents unexpected fields/PII from entering embeddings.
    return (f"Claim {row['claim_id']} | Policy {row['policy_id']} | Product: {row['product']} | "
            f"Status: {row['status']} | Loss: {row['loss_type']} | "
            f"Amount: {row['amount']} {row['currency']}")


def terms(text):
    return set(re.findall(r"[a-z0-9]+", text.lower())) - STOP


class LocalStore:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS docs (id TEXT PRIMARY KEY, text TEXT, payload TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS batches (id TEXT PRIMARY KEY, rows INTEGER, status TEXT)")

    def close(self):
        self.db.close()

    def ingest(self, rows, expected_count, batch_id):
        validate(rows, expected_count)
        staged = stage(rows, batch_id)
        with self.db:
            for row in staged:
                old = self.db.execute("SELECT payload FROM docs WHERE id=?", (row["claim_id"],)).fetchone()
                if old:
                    prior = json.loads(old[0])
                    rank = lambda r: (datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00")), json.dumps({k: r[k] for k in FIELDS}, sort_keys=True))
                    if rank(row) < rank(prior):
                        continue
                self.db.execute("INSERT INTO docs VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET text=excluded.text, payload=excluded.payload",
                                (row["claim_id"], document(row), json.dumps(row)))
            self.db.execute("INSERT OR REPLACE INTO batches VALUES (?, ?, ?)", (batch_id, len(staged), "SUCCEEDED"))
        return len(staged)

    def query(self, question, k=3, product=None):
        if not isinstance(question, str) or not question.strip() or len(question) > 2000:
            raise ValueError("Question must contain 1–2000 characters")
        if not 1 <= k <= 10:
            raise ValueError("k must be between 1 and 10")
        query_terms = terms(question)
        hits = []
        for claim_id, text, payload in self.db.execute("SELECT id, text, payload FROM docs"):
            row = json.loads(payload)
            if product and row["product"].lower() != product.lower():
                continue
            score = len(query_terms & terms(text)) / max(1, len(query_terms))
            if score:
                hits.append({"source_id": claim_id, "text": text, "score": score})
        hits.sort(key=lambda hit: (-hit["score"], hit["source_id"]))
        hits = hits[:k]
        return {"answer": "\n".join(f"[{h['source_id']}] {h['text']}" for h in hits) if hits else "Insufficient evidence in the indexed data.",
                "sources": hits, "mode": "offline lexical retrieval; no language model", "exhaustive": False}
