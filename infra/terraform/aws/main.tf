resource "aws_s3_bucket" "archive" {
  bucket        = var.archive_bucket
  tags          = var.bucket_tags
  force_destroy = false
  lifecycle { prevent_destroy = true }
}

resource "aws_s3_bucket_versioning" "archive" {
  bucket = aws_s3_bucket.archive.id
  versioning_configuration { status = var.versioning_status }
  lifecycle { prevent_destroy = true }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    bucket_key_enabled = var.bucket_key_enabled
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    blocked_encryption_types = var.blocked_encryption_types
  }
  lifecycle { prevent_destroy = true }
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket                  = aws_s3_bucket.archive.id
  block_public_acls       = var.public_access.block_public_acls
  block_public_policy     = var.public_access.block_public_policy
  ignore_public_acls      = var.public_access.ignore_public_acls
  restrict_public_buckets = var.public_access.restrict_public_buckets
  lifecycle { prevent_destroy = true }
}

resource "aws_iam_user" "ingestion" {
  name                 = var.ingestion_user.name
  path                 = var.ingestion_user.path
  permissions_boundary = var.ingestion_user.permissions_boundary
  tags                 = var.ingestion_user.tags
  force_destroy        = false
  lifecycle { prevent_destroy = true }
}

resource "aws_iam_role" "snowflake_reader" {
  name                 = var.reader_role.name
  path                 = var.reader_role.path
  description          = var.reader_role.description
  max_session_duration = var.reader_role.max_session_duration
  permissions_boundary = var.reader_role.permissions_boundary
  assume_role_policy   = var.reader_role.assume_role_policy
  tags                 = var.reader_role.tags
  lifecycle { prevent_destroy = true }
}

resource "aws_iam_user_policy" "ingestion" {
  for_each = var.user_inline_policies
  name     = each.key
  user     = aws_iam_user.ingestion.name
  policy   = each.value
  lifecycle { prevent_destroy = true }
}

resource "aws_iam_role_policy" "snowflake_reader" {
  for_each = var.role_inline_policies
  name     = each.key
  role     = aws_iam_role.snowflake_reader.name
  policy   = each.value
  lifecycle { prevent_destroy = true }
}

resource "aws_iam_policy" "project" {
  for_each    = var.managed_policies
  name        = each.value.name
  path        = each.value.path
  description = each.value.description
  policy      = each.value.policy
  tags        = each.value.tags
  lifecycle { prevent_destroy = true }
}

resource "aws_iam_user_policy_attachment" "ingestion" {
  for_each   = var.user_managed_policy_arns
  user       = aws_iam_user.ingestion.name
  policy_arn = each.value
  depends_on = [aws_iam_policy.project]
  lifecycle { prevent_destroy = true }
}

resource "aws_iam_role_policy_attachment" "snowflake_reader" {
  for_each   = var.role_managed_policy_arns
  role       = aws_iam_role.snowflake_reader.name
  policy_arn = each.value
  depends_on = [aws_iam_policy.project]
  lifecycle { prevent_destroy = true }
}
