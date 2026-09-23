# Deployment and operations

## Prerequisites

1. An existing AWS Databricks workspace and cluster, an approved Unity Catalog stage table, and permission to create a chunk Delta table.
2. A reachable PostgreSQL or Aurora PostgreSQL instance with pgvector. Run `sql/init.sql` with a migration role. Create a separate application role with SELECT/INSERT/UPDATE on `rag_documents`; put a TLS-enforced connection string (`sslmode=verify-full` with the appropriate CA) in a restricted Databricks secret scope.
3. A cluster instance profile permitting `bedrock:InvokeModel` on the selected Titan/chat model ARNs. Set AWS region, verify model access and configure quota/retry behavior for your throughput.
4. An existing Secrets Manager secret shaped as `{"host":"https://<workspace>.cloud.databricks.com","token":"<service-principal-token>"}`. Provision/rotate it outside Terraform. Prefer OAuth M2M for a hardened deployment; this reference adapter uses a secret-managed bearer token.
5. The stage bucket owner enables S3 EventBridge notifications. This repository deliberately does not take ownership of its existing notification configuration.

## Deploy indexing

```sh
python -m pip install build
python -m build
# Authenticate the Databricks CLI normally; do not put credentials into Git.
databricks bundle validate -t dev \
  --var cluster_id=<id>,stage_table=<catalog.schema.table>,chunk_table=<catalog.schema.chunks>,secret_scope=<scope>,vector_secret_key=<key>
databricks bundle deploy -t dev \
  --var cluster_id=<id>,stage_table=<catalog.schema.table>,chunk_table=<catalog.schema.chunks>,secret_scope=<scope>,vector_secret_key=<key>
```

Record the deployed job ID. Supply it and the existing bucket/secret ARN in a local `terraform/dev.tfvars` copied from the example. Use separate AWS accounts and a configured remote state backend with encryption and locking for production; workspaces alone are not a security boundary.

```sh
terraform -chdir=terraform init
terraform -chdir=terraform workspace new dev
# If dev already exists: terraform -chdir=terraform workspace select dev
terraform -chdir=terraform plan -var-file=dev.tfvars -out=dev.tfplan
terraform -chdir=terraform apply dev.tfplan
```

Subscribe your approved destinations to the output SNS topic. Confirm any email subscription through SNS. Customer-managed KMS encrypted secrets require an additional scoped `kms:Decrypt` permission on the Lambda role.

## Trigger contract

After a successful, committed stage update, upload a JSON completion marker under `events/stage/<batch>.json` in the configured bucket. Its arrival triggers indexing of the current approved stage snapshot. The contents are not used to select tables or execute code. Avoid writing incomplete stage markers. The EventBridge S3 event must contain bucket name, object key, and version ID or sequencer. Duplicate deliveries produce the same Databricks idempotency token.

The state machine waits 900 seconds between checks, records the RAG run status in DynamoDB and publishes success/failure to SNS. It does not overwrite upstream raw/stage audit records. Execution timeout is 24 hours. Add CloudWatch alarms for timed-out executions, failed notification delivery and EventBridge delivery errors before production; a workflow timeout does not pass through the failure catch state. If a job fails, fix its cause and emit a new object version to request a new run.

## Query

Run notebook 03 under an authorized workspace user, with `question`, `product`, `secret_scope`, `vector_secret_key`, `chat_model_id` widgets. It calls notebook 04 to record metrics. Install the built wheel, cloud extras and MLflow on the query cluster.

`cloud/query_handler.py` is an optional API integration, not provisioned by the Terraform module. Package it with the wheel/cloud dependencies in a Linux Lambda artifact; deploy in a VPC with database reachability and required service endpoints. Configure `VECTOR_SECRET_ARN` (JSON `dsn`), `BEDROCK_CHAT_MODEL_ID`, AWS region, a JWT authorizer and least-privilege IAM. Invoke through the authorizer with `{"question":"Show flood claims"}`. Do not configure a public unauthenticated function URL.

## Scale and lifecycle limits

- The notebook streams to the driver and embeds sequentially. It is an understandable small-scale reference, not a measured multi-terabyte implementation. Add partitioned work queues, Bedrock quota controls, checkpointing and bulk vector ingestion for large volumes.
- Batch chunk snapshots append to Delta; reruns deduplicate claim IDs during embedding. Configure retention cleanup. Snapshot reads are consistent within the Spark job; serialize indexing jobs (`max_concurrent_runs: 1`).
- Upserts preserve newer source timestamps. Source deletion/tombstone propagation is not implemented: explicitly delete affected vector rows through an audited retention job before claiming deletion compliance.
- Row-level retrieval is not an analytics engine. Use governed SQL for sums, counts and portfolio metrics. Do not derive exhaustive totals from top-k retrieval.
- A similarity threshold is a heuristic; evaluate against labeled questions before choosing it. Grounding instructions reduce but cannot eliminate hallucinations or prompt injection. Verify citations and claims before operational decisions.
- The implementation excludes PII from embeddings; upstream Vault encryption remains a prerequisite for the main data platform. Automatic PII discovery, fuzzy entity resolution and AI failure summaries from the source document are future extensions.

## Acceptance checklist

Run a synthetic stage batch, verify five expected claim documents, replay the same marker, inspect the single Databricks run ID, observe terminal audit status, receive SNS notification and query a known claim. Also test failed Databricks runs, older versions, empty evidence, wrong product scope, absent authorizer, denied Bedrock permissions and unreachable database. Record actual latency/retrieval quality; do not represent local mocked tests as cloud certification.
