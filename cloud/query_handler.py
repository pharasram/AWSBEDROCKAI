"""Optional Lambda API handler; deploy behind a JWT authorizer and private DB access."""
import json
import os
from insurance_rag.cloud import Bedrock, VectorStore


def main(event, context):
    claims = event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
    product = claims.get("custom:product")
    if not claims.get("sub") or product not in {"motor", "home", "travel"}:
        return response(403, {"error": "A verified product scope is required"})
    try:
        payload = json.loads(event.get("body") or "{}")
        question = payload.get("question", "")
        if not isinstance(question, str) or not 1 <= len(question.strip()) <= 2000:
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        return response(400, {"error": "Supply a question of 1–2000 characters"})
    import boto3
    secret = json.loads(boto3.client("secretsmanager").get_secret_value(
        SecretId=os.environ["VECTOR_SECRET_ARN"])["SecretString"])
    store = VectorStore(secret["dsn"])
    try:
        model = Bedrock()
        sources = store.search(model.embed(question), product)
        return response(200, {"answer": model.answer(question, sources), "sources": sources,
                              "exhaustive": False})
    finally:
        store.close()


def response(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
            "body": json.dumps(body)}
