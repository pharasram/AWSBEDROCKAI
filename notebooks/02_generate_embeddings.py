# Databricks notebook source
from insurance_rag.cloud import Bedrock, VectorStore
from insurance_rag.core import document
from pyspark.sql import functions as F

# Serialize source timestamps with an explicit UTC offset before leaving Spark.
spark.conf.set("spark.sql.session.timeZone", "UTC")

for name in ("chunk_table", "batch_id", "secret_scope", "vector_secret_key"):
    dbutils.widgets.text(name, "")
chunk_table, batch_id, scope, key = [dbutils.widgets.get(n) for n in ("chunk_table", "batch_id", "secret_scope", "vector_secret_key")]
dsn = dbutils.secrets.get(scope, key)
model, store = Bedrock(), VectorStore(dsn)
count = 0
try:
    # Streams rows to the driver without collect(); bounded reference implementation.
    # For large estates, partition this workload with rate limiting and a durable work queue.
    # The preceding pipeline task publishes the current materialized snapshot.
    # batch_id is an audit identifier, not a filter on this current-state table.
    rows = spark.table(chunk_table).withColumn(
        "updated_at", F.date_format("updated_at", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))
    for record in rows.toLocalIterator():
        row = record.asDict()
        row["updated_at"] = row["updated_at"].isoformat() if hasattr(row["updated_at"], "isoformat") else row["updated_at"]
        store.upsert(row, model.embed(document(row)), batch_id)
        count += 1
finally:
    store.close()
dbutils.jobs.taskValues.set(key="indexed_count", value=count)
