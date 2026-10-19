terraform {
  required_version = "= 1.14.5"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "= 2.22.0"
    }
  }
  backend "s3" {}
}

# Authentication is supplied through SNOWFLAKE_* environment variables.
# Never use file(private_key_path) in HCL or pass private keys as variables.
provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  role              = var.admin_role
}
