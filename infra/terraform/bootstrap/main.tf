variable "aws_profile" { type = string }
variable "account_id" { type = string }
variable "region" { type = string }
variable "state_bucket" { type = string }
variable "state_admin_arn" {
  description = "Existing IAM user/role ARN allowed to read/write state; never an STS session ARN."
  type        = string
}

resource "aws_s3_bucket" "state" {
  bucket        = var.state_bucket
  force_destroy = false
  lifecycle { prevent_destroy = true }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket policy adds an explicit boundary even if an ingestion user's own policy
# is broader. The administrator retains recovery access through IAM/account root.
resource "aws_s3_bucket_policy" "state" {
  bucket = aws_s3_bucket.state.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]
        Condition = { Bool = { "aws:SecureTransport" = "false" } }
      },
      {
        Sid       = "RestrictStateToAdministrator"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource  = [aws_s3_bucket.state.arn, "${aws_s3_bucket.state.arn}/*"]
        Condition = { ArnNotEquals = { "aws:PrincipalArn" = [var.state_admin_arn, "arn:aws:iam::${var.account_id}:root"] } }
      }
    ]
  })
  lifecycle { prevent_destroy = true }
}

output "state_bucket" { value = aws_s3_bucket.state.id }
