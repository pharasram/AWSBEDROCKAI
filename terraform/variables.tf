variable "region" {
  type    = string
  default = "us-east-1"
}
variable "prefix" {
  type    = string
  default = "insurance-rag"
}
variable "stage_bucket" {
  description = "Existing bucket with EventBridge delivery enabled by its owner"
  type        = string
}
variable "databricks_job_id" {
  type = string
}
variable "databricks_secret_arn" {
  description = "Existing Secrets Manager secret containing host and token; never set secret values in Terraform"
  type        = string
}
