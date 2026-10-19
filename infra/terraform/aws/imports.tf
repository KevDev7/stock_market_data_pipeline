import {
  for_each = var.adopt_existing ? toset([var.archive_bucket]) : toset([])
  to       = aws_s3_bucket.archive
  id       = each.value
}
import {
  for_each = var.adopt_existing ? toset([var.archive_bucket]) : toset([])
  to       = aws_s3_bucket_versioning.archive
  id       = each.value
}
import {
  for_each = var.adopt_existing ? toset([var.archive_bucket]) : toset([])
  to       = aws_s3_bucket_server_side_encryption_configuration.archive
  id       = each.value
}
import {
  for_each = var.adopt_existing ? toset([var.archive_bucket]) : toset([])
  to       = aws_s3_bucket_public_access_block.archive
  id       = each.value
}
import {
  for_each = var.adopt_existing ? toset([var.ingestion_user.name]) : toset([])
  to       = aws_iam_user.ingestion
  id       = each.value
}
import {
  for_each = var.adopt_existing ? toset([var.reader_role.name]) : toset([])
  to       = aws_iam_role.snowflake_reader
  id       = each.value
}
import {
  for_each = var.adopt_existing ? var.user_inline_policies : {}
  to       = aws_iam_user_policy.ingestion[each.key]
  id       = "${var.ingestion_user.name}:${each.key}"
}
import {
  for_each = var.adopt_existing ? var.role_inline_policies : {}
  to       = aws_iam_role_policy.snowflake_reader[each.key]
  id       = "${var.reader_role.name}:${each.key}"
}
import {
  for_each = var.adopt_existing ? var.managed_policies : {}
  to       = aws_iam_policy.project[each.key]
  id       = "arn:aws:iam::${var.account_id}:policy${each.value.path}${each.value.name}"
}
import {
  for_each = var.adopt_existing ? var.user_managed_policy_arns : toset([])
  to       = aws_iam_user_policy_attachment.ingestion[each.value]
  id       = "${var.ingestion_user.name}/${each.value}"
}
import {
  for_each = var.adopt_existing ? var.role_managed_policy_arns : toset([])
  to       = aws_iam_role_policy_attachment.snowflake_reader[each.value]
  id       = "${var.reader_role.name}/${each.value}"
}
