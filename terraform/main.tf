terraform {
  required_version = ">= 1.6, < 2.0"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.0" }
  }
}
provider "aws" { region = var.region }
locals { name = "${var.prefix}-${terraform.workspace}" }
resource "aws_dynamodb_table" "audit" {
  name         = local.name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "batch_id"
  attribute {
    name = "batch_id"
    type = "S"
  }
  point_in_time_recovery { enabled = true }
  server_side_encryption { enabled = true }
}
resource "aws_sns_topic" "status" { name = "${local.name}-status" }
resource "aws_iam_role" "lambda" {
  name = "${local.name}-lambda"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole"
  }] })
}
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name}"
  retention_in_days = 30
}
resource "aws_iam_role_policy" "lambda" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.lambda.arn}:*" },
    { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = var.databricks_secret_arn },
    { Effect = "Allow", Action = ["dynamodb:UpdateItem"], Resource = aws_dynamodb_table.audit.arn }
  ] })
}
data "archive_file" "code" {
  type        = "zip"
  source_file = "${path.module}/../cloud/handler.py"
  output_path = "${path.module}/cloud.zip"
}
resource "aws_lambda_function" "trigger" {
  function_name    = local.name
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "handler.main"
  filename         = data.archive_file.code.output_path
  source_code_hash = data.archive_file.code.output_base64sha256
  timeout          = 60
  environment {
    variables = {
      DATABRICKS_SECRET_ARN = var.databricks_secret_arn
      DATABRICKS_JOB_ID     = var.databricks_job_id
      AUDIT_TABLE           = aws_dynamodb_table.audit.name
      STAGE_BUCKET          = var.stage_bucket
    }
  }
  depends_on = [aws_iam_role_policy.lambda, aws_cloudwatch_log_group.lambda]
}
resource "aws_iam_role" "workflow" {
  name = "${local.name}-workflow"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Principal = { Service = "states.amazonaws.com" }, Action = "sts:AssumeRole"
  }] })
}
resource "aws_iam_role_policy" "workflow" {
  role = aws_iam_role.workflow.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["lambda:InvokeFunction"], Resource = aws_lambda_function.trigger.arn },
    { Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.status.arn }
  ] })
}
resource "aws_sfn_state_machine" "rag" {
  name     = local.name
  role_arn = aws_iam_role.workflow.arn
  definition = templatefile("${path.module}/workflow.json.tftpl", {
    lambda_arn = aws_lambda_function.trigger.arn, topic_arn = aws_sns_topic.status.arn
  })
  depends_on = [aws_iam_role_policy.workflow]
}
resource "aws_iam_role" "event" {
  name = "${local.name}-event"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Principal = { Service = "events.amazonaws.com" }, Action = "sts:AssumeRole"
  }] })
}
resource "aws_iam_role_policy" "event" {
  role = aws_iam_role.event.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = ["states:StartExecution"], Resource = aws_sfn_state_machine.rag.arn
  }] })
}
resource "aws_cloudwatch_event_rule" "stage" {
  name = "${local.name}-stage"
  event_pattern = jsonencode({ source = ["aws.s3"], "detail-type" = ["Object Created"],
    detail = { bucket = { name = [var.stage_bucket] }, object = { key = [{ prefix = "events/stage/" }] } }
  })
}
resource "aws_cloudwatch_event_target" "stage" {
  rule       = aws_cloudwatch_event_rule.stage.name
  arn        = aws_sfn_state_machine.rag.arn
  role_arn   = aws_iam_role.event.arn
  depends_on = [aws_iam_role_policy.event]
}
output "workflow_arn" { value = aws_sfn_state_machine.rag.arn }
output "notification_topic_arn" { value = aws_sns_topic.status.arn }
