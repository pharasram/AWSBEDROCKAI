# Databricks notebook source
from insurance_rag.cloud import Bedrock, VectorStore
import os
import time

for name in ("question", "product", "secret_scope", "vector_secret_key", "chat_model_id"):
    dbutils.widgets.text(name, "")
question, product, scope, key, model_id = [dbutils.widgets.get(n) for n in ("question", "product", "secret_scope", "vector_secret_key", "chat_model_id")]
if not question.strip() or len(question) > 2000 or product not in {"motor", "home", "travel"}:
    raise ValueError("Question and approved product required")
# This notebook is an operator tool; workspace permissions must restrict who can run it.
os.environ["BEDROCK_CHAT_MODEL_ID"] = model_id
store, model = VectorStore(dbutils.secrets.get(scope, key)), Bedrock()
started = time.monotonic()
try:
    evidence = store.search(model.embed(question), product)
    answer = model.answer(question, evidence)
finally:
    store.close()
print(answer)
latency_ms = (time.monotonic() - started) * 1000

# COMMAND ----------
# MAGIC %run ./04_mlflow_tracking

# COMMAND ----------
track_query(model_id, latency_ms, evidence)
