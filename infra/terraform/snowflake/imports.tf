locals {
  quoted_database = "\"${var.database_name}\""
  quoted_raw      = "${local.quoted_database}.\"${var.raw_schema_name}\""
  grant_import_ids = {
    for key, grant in var.grants : key => join("|", concat(
      ["\"${grant.role}\"", tostring(grant.grant_option), "false", join(",", sort(tolist(grant.privileges)))],
      grant.scope == "account_object" ? ["OnAccountObject", grant.object_type, grant.object_name] :
      grant.scope == "schema" ? ["OnSchema", "OnSchema", grant.object_name] :
      grant.scope == "future" ? ["OnSchemaObject", "OnFuture", grant.object_type, "InSchema", grant.object_name] :
      ["OnSchemaObject", "OnObject", grant.object_type, grant.object_name]
    ))
  }
}
import {
  for_each = var.adopt_existing ? toset([local.quoted_database]) : toset([])
  to       = snowflake_database.market
  id       = each.value
}
import {
  for_each = var.adopt_existing ? toset([local.quoted_raw]) : toset([])
  to       = snowflake_schema.raw
  id       = each.value
}
import {
  for_each = var.adopt_existing ? toset([var.storage_integration_name]) : toset([])
  to       = snowflake_storage_integration_aws.archive
  id       = "\"${each.value}\""
}
import {
  for_each = var.adopt_existing ? toset([var.file_format_name]) : toset([])
  to       = snowflake_file_format_json.prices
  id       = "${local.quoted_raw}.\"${each.value}\""
}
import {
  for_each = var.adopt_existing ? toset([var.price_stage_name]) : toset([])
  to       = snowflake_stage_external_s3.prices
  id       = "${local.quoted_raw}.\"${each.value}\""
}
import {
  for_each = var.adopt_existing ? toset([var.reference_stage_name]) : toset([])
  to       = snowflake_stage_external_s3.reference
  id       = "${local.quoted_raw}.\"${each.value}\""
}
import {
  for_each = var.adopt_existing ? var.warehouses : {}
  to       = snowflake_warehouse.project[each.key]
  id       = "\"${each.value.name}\""
}
import {
  for_each = var.adopt_existing ? var.roles : {}
  to       = snowflake_account_role.project[each.key]
  id       = "\"${each.value.name}\""
}
import {
  for_each = var.adopt_existing && var.manage_service_users ? var.service_users : {}
  to       = snowflake_service_user.project[each.key]
  id       = "\"${each.value.name}\""
}
import {
  for_each = var.adopt_existing ? var.role_memberships : {}
  to       = snowflake_grant_account_role.user[each.key]
  id       = "\"${each.value.role}\"|USER|\"${each.value.user}\""
}
import {
  for_each = var.adopt_existing ? var.grants : {}
  to       = snowflake_grant_privileges_to_account_role.project[each.key]
  id       = local.grant_import_ids[each.key]
}
