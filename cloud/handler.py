"""Lambda: stage-completion event -> idempotent Databricks run; poll separately."""
import hashlib
import json
import os
import urllib.request


def databricks(path, payload=None):
    import boto3
    secret = json.loads(boto3.client("secretsmanager").get_secret_value(
        SecretId=os.environ["DATABRICKS_SECRET_ARN"])["SecretString"])
    host = secret["host"].rstrip("/")
    from urllib.parse import urlparse
    parsed = urlparse(host)
    if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(".cloud.databricks.com"):
        raise ValueError("Expected an AWS Databricks HTTPS workspace host")
    request = urllib.request.Request(host + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Authorization": "Bearer " + secret["token"], "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def main(event, context):
    import boto3
    table = boto3.resource("dynamodb").Table(os.environ["AUDIT_TABLE"])
    if event.get("action") == "poll":
        batch_id, run_id = event["batch_id"], int(event["run_id"])
        run = databricks(f"/api/2.2/jobs/runs/get?run_id={run_id}")
        state = run["state"]
        terminal = state["life_cycle_state"] in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
        status = "SUCCEEDED" if state.get("result_state") == "SUCCESS" else "FAILED" if terminal else "RUNNING"
        table.update_item(Key={"batch_id": batch_id}, UpdateExpression="SET #s=:s, run_id=:r",
                          ExpressionAttributeNames={"#s": "status"},
                          ExpressionAttributeValues={":s": status, ":r": run_id})
        return {"batch_id": batch_id, "run_id": run_id, "status": status}
    detail = event["detail"]
    bucket, key = detail["bucket"]["name"], detail["object"]["key"]
    if bucket != os.environ["STAGE_BUCKET"] or not key.startswith("events/stage/") or not key.endswith(".json"):
        raise ValueError("Unexpected stage event")
    # Stable across delivery retries; a new object version creates a new run.
    version = detail["object"].get("version-id") or detail["object"].get("sequencer")
    if not version:
        raise ValueError("Object version or sequencer required")
    batch_id = hashlib.sha256(f"{bucket}/{key}/{version}".encode()).hexdigest()
    run = databricks("/api/2.2/jobs/run-now", {"job_id": int(os.environ["DATABRICKS_JOB_ID"]),
        "idempotency_token": batch_id, "job_parameters": {"batch_id": batch_id}})
    # Do not reset a completed audit record on duplicate event delivery.
    table.update_item(Key={"batch_id": batch_id},
        UpdateExpression="SET #s=if_not_exists(#s,:s), run_id=:r",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": "RUNNING", ":r": run["run_id"]})
    return {"batch_id": batch_id, "run_id": run["run_id"], "status": "RUNNING"}
