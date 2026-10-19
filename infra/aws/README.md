# Historical deployment references

The old CloudFormation and policy files have moved to
[the historical archive](../legacy/README.md). They are not deployment entrypoints.
The active configuration is in [Terraform](../terraform/README.md).

Existing resources were imported, not recreated. Do not deploy a CloudFormation
stack over them or edit IAM policies independently of their Terraform owner.
