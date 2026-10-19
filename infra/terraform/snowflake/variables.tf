variable "organization_name" { type = string }
variable "adopt_existing" {
  description = "True for this existing deployment. False only for an explicitly separate new environment."
  type        = bool
  default     = true
}
variable "manage_service_users" {
  description = "Explicit opt-in for dedicated service-user management. Existing-user adoption requires the reviewed metadata-only import guard; fresh provisioning does not use that exception."
  type        = bool
  default     = false
}
variable "account_name" { type = string }
variable "admin_role" { type = string }
variable "database_name" { type = string }
variable "database_comment" { type = string }
variable "retention_days" { type = number }
variable "raw_retention_days" { type = number }
variable "raw_schema_name" { type = string }
variable "raw_schema_comment" { type = string }
variable "archive_bucket" { type = string }
variable "price_prefix" { type = string }
variable "reference_prefix" { type = string }
variable "storage_role_arn" { type = string }
variable "storage_integration_name" { type = string }
variable "storage_integration_comment" { type = string }
variable "storage_external_id" {
  description = "Existing integration external ID, supplied privately to preserve trust. Null for a new integration."
  type        = string
  default     = null
  sensitive   = true
}
variable "price_stage_name" { type = string }
variable "reference_stage_name" { type = string }
variable "file_format_name" { type = string }
variable "warehouses" {
  description = "Only project-dedicated warehouses; shared COMPUTE_WH is not managed."
  type = map(object({
    name                                = string
    size                                = string
    auto_suspend                        = number
    auto_resume                         = bool
    generation                          = string
    resource_monitor                    = optional(string)
    enable_query_acceleration           = bool
    query_acceleration_max_scale_factor = number
    comment                             = string
  }))
}
variable "roles" {
  type = map(object({ name = string, comment = string }))
}
variable "service_users" {
  description = "Dedicated service users only; no passwords or private keys. Public-key registration stays manual."
  type = map(object({
    name                           = string
    login_name                     = string
    display_name                   = string
    disabled                       = string
    default_role                   = string
    default_warehouse              = string
    default_namespace              = string
    default_secondary_roles_option = string
    comment                        = string
  }))
}
variable "role_memberships" {
  type = map(object({ role = string, user = string }))
}
variable "grants" {
  description = "Explicit existing project grants only. No ownership, broad all-object grants, or account-wide grants."
  type = map(object({
    role         = string
    privileges   = set(string)
    scope        = string
    object_type  = string
    object_name  = string
    grant_option = bool
  }))
  validation {
    condition = alltrue([for g in values(var.grants) :
      contains(["account_object", "schema", "schema_object", "future"], g.scope)
      && !contains(g.privileges, "OWNERSHIP")
    ])
    error_message = "Use project object/future grants only; do not adopt ownership or account-wide privileges."
  }
}
