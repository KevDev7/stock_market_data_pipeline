resource "snowflake_database" "market" {
  name                        = var.database_name
  comment                     = var.database_comment
  data_retention_time_in_days = var.retention_days
  lifecycle { prevent_destroy = true }
}

# RAW is an infrastructure namespace for stages; ingestion still owns its tables.
# No STAGING/INTERMEDIATE/MARTS schema resources: dbt/cutovers own those namespaces.
resource "snowflake_schema" "raw" {
  database                    = snowflake_database.market.name
  name                        = var.raw_schema_name
  comment                     = var.raw_schema_comment
  is_transient                = false
  with_managed_access         = "false"
  data_retention_time_in_days = var.raw_retention_days
  lifecycle { prevent_destroy = true }
}

resource "snowflake_storage_integration_aws" "archive" {
  name                    = var.storage_integration_name
  comment                 = var.storage_integration_comment
  enabled                 = true
  storage_provider        = "S3"
  storage_aws_role_arn    = var.storage_role_arn
  storage_aws_external_id = var.storage_external_id
  storage_allowed_locations = [
    "s3://${var.archive_bucket}/${trim(var.price_prefix, "/")}/",
    "s3://${var.archive_bucket}/${trim(var.reference_prefix, "/")}/",
  ]
  lifecycle {
    prevent_destroy = true
    # Preserve the generated trust identity rather than resetting it on import.
    ignore_changes = [storage_aws_external_id]
  }
}

resource "snowflake_file_format_json" "prices" {
  database         = snowflake_database.market.name
  schema           = snowflake_schema.raw.name
  name             = var.file_format_name
  compression      = "AUTO"
  binary_format    = "HEX"
  date_format      = "AUTO"
  time_format      = "AUTO"
  timestamp_format = "AUTO"
  null_if          = []
  lifecycle { prevent_destroy = true }
}

resource "snowflake_stage_external_s3" "prices" {
  database            = snowflake_database.market.name
  schema              = snowflake_schema.raw.name
  name                = var.price_stage_name
  url                 = "s3://${var.archive_bucket}/${trim(var.price_prefix, "/")}/"
  storage_integration = snowflake_storage_integration_aws.archive.name
  directory {
    enable = false
  }
  file_format {
    format_name = snowflake_file_format_json.prices.fully_qualified_name
  }
  lifecycle { prevent_destroy = true }
}

resource "snowflake_stage_external_s3" "reference" {
  database            = snowflake_database.market.name
  schema              = snowflake_schema.raw.name
  name                = var.reference_stage_name
  url                 = "s3://${var.archive_bucket}/${trim(var.reference_prefix, "/")}/"
  storage_integration = snowflake_storage_integration_aws.archive.name
  directory {
    enable = false
  }
  file_format {
    json {
      compression      = "AUTO"
      binary_format    = "HEX"
      date_format      = "AUTO"
      time_format      = "AUTO"
      timestamp_format = "AUTO"
      null_if          = []
    }
  }
  lifecycle { prevent_destroy = true }
}

resource "snowflake_warehouse" "project" {
  for_each                            = var.warehouses
  name                                = each.value.name
  warehouse_size                      = each.value.size
  warehouse_type                      = "STANDARD"
  min_cluster_count                   = 1
  max_cluster_count                   = 1
  scaling_policy                      = "STANDARD"
  auto_suspend                        = each.value.auto_suspend
  auto_resume                         = each.value.auto_resume
  generation                          = each.value.generation
  resource_monitor                    = each.value.resource_monitor
  enable_query_acceleration           = each.value.enable_query_acceleration
  query_acceleration_max_scale_factor = each.value.query_acceleration_max_scale_factor
  comment                             = each.value.comment
  initially_suspended                 = true
  lifecycle { prevent_destroy = true }
}

resource "snowflake_account_role" "project" {
  for_each = var.roles
  name     = each.value.name
  comment  = each.value.comment
  lifecycle { prevent_destroy = true }
}

resource "snowflake_service_user" "project" {
  for_each                       = var.manage_service_users ? var.service_users : {}
  name                           = each.value.name
  login_name                     = each.value.login_name
  display_name                   = each.value.display_name
  disabled                       = each.value.disabled
  default_role                   = each.value.default_role
  default_warehouse              = each.value.default_warehouse
  default_namespace              = each.value.default_namespace
  default_secondary_roles_option = each.value.default_secondary_roles_option
  comment                        = each.value.comment
  depends_on                     = [snowflake_account_role.project, snowflake_warehouse.project]
  lifecycle {
    prevent_destroy = true
    # Adoption must never clear/rotate the existing authentication keys.
    ignore_changes = [rsa_public_key, rsa_public_key_2, mins_to_unlock, email]
  }
}

resource "snowflake_grant_account_role" "user" {
  for_each   = var.role_memberships
  role_name  = each.value.role
  user_name  = each.value.user
  depends_on = [snowflake_account_role.project, snowflake_service_user.project]
  lifecycle { prevent_destroy = true }
}

resource "snowflake_grant_privileges_to_account_role" "project" {
  for_each          = var.grants
  account_role_name = each.value.role
  privileges        = each.value.privileges
  with_grant_option = each.value.grant_option
  always_apply      = false
  depends_on        = [snowflake_database.market, snowflake_account_role.project, snowflake_warehouse.project]
  dynamic "on_account_object" {
    for_each = each.value.scope == "account_object" ? [each.value] : []
    content {
      object_type = on_account_object.value.object_type
      object_name = on_account_object.value.object_name
    }
  }
  dynamic "on_schema" {
    for_each = each.value.scope == "schema" ? [each.value] : []
    content { schema_name = on_schema.value.object_name }
  }
  dynamic "on_schema_object" {
    for_each = contains(["schema_object", "future"], each.value.scope) ? [each.value] : []
    content {
      object_type = each.value.scope == "schema_object" ? each.value.object_type : null
      object_name = each.value.scope == "schema_object" ? each.value.object_name : null
      dynamic "future" {
        for_each = each.value.scope == "future" ? [each.value] : []
        content {
          object_type_plural = future.value.object_type
          in_schema          = future.value.object_name
        }
      }
    }
  }
  lifecycle { prevent_destroy = true }
}

output "storage_iam_principal" {
  value = snowflake_storage_integration_aws.archive.describe_output[0].iam_user_arn
}
output "storage_external_id" {
  value     = snowflake_storage_integration_aws.archive.describe_output[0].external_id
  sensitive = true
}
