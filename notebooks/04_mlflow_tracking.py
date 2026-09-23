# Databricks notebook source
import mlflow


def track_query(model_id, latency_ms, evidence):
    # Do not log raw questions, context or answers: these can contain sensitive data.
    with mlflow.start_run(run_name="insurance-rag-query"):
        mlflow.log_param("chat_model", model_id)
        mlflow.log_param("embedding_model", "amazon.titan-embed-text-v2:0")
        mlflow.log_metric("latency_ms", latency_ms)
        mlflow.log_metric("retrieved_count", len(evidence))
        mlflow.log_metric("top_score", max((s["score"] for s in evidence), default=0))
