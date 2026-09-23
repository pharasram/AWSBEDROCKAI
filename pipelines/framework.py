"""Validated feed metadata and pure Spark transformations; no data actions or I/O."""
import json
import re
from pathlib import Path

APPROVED_FIELDS = {"claim_id", "policy_id", "product", "status", "loss_type", "amount", "currency", "updated_at"}


def load_contracts(path=None):
    path = path or Path(__file__).parent / "config" / "datasets.json"
    datasets = json.loads(Path(path).read_text(encoding="utf-8"))["datasets"]
    if not datasets:
        raise ValueError("At least one dataset is required")
    names = set()
    for config in datasets:
        name = config["name"]
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name) or name in names:
            raise ValueError("Dataset names must be unique SQL identifiers")
        names.add(name)
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", config["source_subpath"]):
            raise ValueError("source_subpath must be one safe path segment")
        if set(config["fields"]) != APPROVED_FIELDS:
            raise ValueError("Only the reviewed claim fields may be embedded")
        if config["keys"] != ["claim_id"] or config["sequence_column"] != "updated_at":
            raise ValueError("Claim contract requires claim_id and updated_at")
        expected_types = {key: "string" for key in APPROVED_FIELDS}
        expected_types.update(amount="decimal(18,2)", updated_at="timestamp")
        if config["fields"] != expected_types:
            raise ValueError("Unsupported contract types")
        if set(config["required"]) != APPROVED_FIELDS - {"amount", "updated_at"}:
            raise ValueError("All descriptive fields must be required")
        for key in ("products", "statuses", "loss_types", "currencies"):
            if not config[key] or not all(isinstance(v, str) and re.fullmatch(r"[A-Za-z0-9 _-]+", v) for v in config[key]):
                raise ValueError("Expected a non-empty reviewed categorical allowlist")
    return datasets


def raw_schema(config):
    # Read source values as strings first; try_cast routes bad types to quarantine.
    return ", ".join(f"{name} STRING" for name in config["fields"]) + ", _rescued_data STRING, _corrupt_record STRING"


def quality_rules(config):
    rules = {f"required_{name}": f"{name} IS NOT NULL AND length(trim({name})) > 0" for name in config["required"]}
    rules.update({
        "valid_amount": "amount IS NOT NULL AND amount >= 0",
        "valid_timestamp": "updated_at IS NOT NULL AND _source_updated_at RLIKE '(Z|[+-][0-9]{2}:[0-9]{2})$'",
        "clean_schema": "_rescued_data IS NULL AND _corrupt_record IS NULL",
        "pseudonymous_claim": "claim_id RLIKE '^CLM-[A-Za-z0-9-]+$'",
        "pseudonymous_policy": "policy_id RLIKE '^POL-[A-Za-z0-9-]+$'",
    })
    for field, key in (("product", "products"), ("status", "statuses"), ("loss_type", "loss_types"), ("currency", "currencies")):
        values = ", ".join("'" + value + "'" for value in config[key])
        rules[f"approved_{field}"] = f"{field} IN ({values})"
    return rules


def validity_expression(config):
    # NULL must be invalid so the valid/quarantine branches cover every row.
    return " AND ".join(f"coalesce(({rule}), false)" for rule in quality_rules(config).values())


def normalize(frame, config):
    from pyspark.sql import functions as F
    result = frame.withColumn("_source_updated_at", F.col("updated_at"))
    for name, dtype in config["fields"].items():
        result = result.withColumn(name, F.expr(f"try_cast(`{name}` AS {dtype})"))
    payload = F.to_json(F.struct(*[F.col(name) for name in sorted(config["fields"])]))
    return (result.withColumn("row_hash_key", F.sha2(payload, 256))
            .withColumn("source_code", F.lit(config["source_code"]))
            .withColumn("load", F.lit("upsert"))
            .withColumn("stage_load_ts", F.current_timestamp())
            .withColumn("transaction_correlation_id", F.sha2(F.concat_ws("|", F.col("_source_file"), F.col("row_hash_key")), 256)))


def rag_projection(frame, config):
    from pyspark.sql import functions as F
    # Never include rescued data, free text, source paths or diagnostics.
    return frame.select(*config["fields"], "row_hash_key").withColumn(
        "content", F.concat_ws(" | ",
            F.concat(F.lit("Claim "), F.col("claim_id")),
            F.concat(F.lit("Policy "), F.col("policy_id")),
            F.concat(F.lit("Product: "), F.col("product")),
            F.concat(F.lit("Status: "), F.col("status")),
            F.concat(F.lit("Loss: "), F.col("loss_type")),
            F.concat(F.lit("Amount: "), F.col("amount"), F.lit(" "), F.col("currency"))))
