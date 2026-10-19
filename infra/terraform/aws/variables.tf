variable "aws_profile" { type = string }
variable "adopt_existing" {
  description = "True for this existing deployment. False only for an explicitly separate new environment."
  type        = bool
  default     = true
}
variable "account_id" { type = string }
variable "region" { type = string }
variable "archive_bucket" { type = string }
variable "bucket_tags" { type = map(string) }
variable "versioning_status" { type = string }
variable "bucket_key_enabled" { type = bool }
variable "blocked_encryption_types" { type = list(string) }
variable "public_access" {
  type = object({
    block_public_acls       = bool
    block_public_policy     = bool
    ignore_public_acls      = bool
    restrict_public_buckets = bool
  })
}
variable "ingestion_user" {
  type = object({
    name                 = string
    path                 = string
    permissions_boundary = optional(string)
    tags                 = map(string)
  })
}
variable "reader_role" {
  type = object({
    name                 = string
    path                 = string
    description          = string
    max_session_duration = number
    permissions_boundary = optional(string)
    assume_role_policy   = string
    tags                 = map(string)
  })
}
variable "user_inline_policies" {
  description = "Actual policy names mapped to JSON documents, copied from live inventory."
  type        = map(string)
}
variable "role_inline_policies" { type = map(string) }
variable "managed_policies" {
  description = "Only project-owned customer-managed policies. Shared policies are referenced by ARN."
  type        = map(object({ name = string, path = string, description = string, policy = string, tags = map(string) }))
  default     = {}
}
variable "user_managed_policy_arns" {
  type    = set(string)
  default = []
}
variable "role_managed_policy_arns" {
  type    = set(string)
  default = []
}
