# Databricks notebook source
# LEGACY EXAMPLE ONLY: the deployed job now uses pipelines/transformations/insurance.py.
# Do not run this notebook against pipeline-owned materialized views or streaming tables.
# Install the built insurance_rag wheel as a job library.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

for name in ("stage_table", "chunk_table", "batch_id"):
    dbutils.widgets.text(name, "")
stage_table, chunk_table, batch_id = [dbutils.widgets.get(n) for n in ("stage_table", "chunk_table", "batch_id")]
if not all((stage_table, chunk_table, batch_id)):
    raise ValueError("stage_table, chunk_table and batch_id are required")

# Consume only approved staged fields. PII/Vault ciphertext and free-text notes are excluded.
columns = ["claim_id", "policy_id", "product", "status", "loss_type", "amount", "currency", "updated_at"]
source = spark.table(stage_table).select(*columns)
if source.where(F.col("claim_id").isNull() | F.col("updated_at").isNull()).limit(1).count():
    raise ValueError("Missing source identity or timestamp")
source = source.withColumn("_tie", F.to_json(F.struct(*sorted(columns))))
window = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc(), F.col("_tie").desc())
chunks = source.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn", "_tie")
# A structured claim is a bounded chunk; do not split identifiers across text fragments.
chunks = chunks.withColumn("batch_id", F.lit(batch_id))
# Full snapshot, partition isolated per run. Enable retention cleanup for old batches.
chunks.write.format("delta").mode("append").saveAsTable(chunk_table)
