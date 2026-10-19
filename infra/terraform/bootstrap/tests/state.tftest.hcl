mock_provider "aws" {}

variables {
  aws_profile     = "test"
  account_id      = "123456789012"
  region          = "us-east-1"
  state_bucket    = "stock-market-test-state-123456789012"
  state_admin_arn = "arn:aws:iam::123456789012:role/test-infrastructure-admin"
}

run "state_security" {
  command = plan
  assert {
    condition     = aws_s3_bucket.state.force_destroy == false
    error_message = "State bucket must never allow force deletion."
  }
  assert {
    condition     = aws_s3_bucket_versioning.state.versioning_configuration[0].status == "Enabled"
    error_message = "State needs recoverable versions."
  }
  assert {
    condition     = aws_s3_bucket_public_access_block.state.block_public_acls && aws_s3_bucket_public_access_block.state.block_public_policy && aws_s3_bucket_public_access_block.state.ignore_public_acls && aws_s3_bucket_public_access_block.state.restrict_public_buckets
    error_message = "State bucket must block all public access."
  }
  assert {
    condition     = one(aws_s3_bucket_server_side_encryption_configuration.state.rule).apply_server_side_encryption_by_default[0].sse_algorithm == "AES256"
    error_message = "State must use encryption at rest."
  }
}
