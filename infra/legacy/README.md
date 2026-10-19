# Historical infrastructure references

The AWS templates/policies here describe the pre-Terraform manual deployment.
They are retained for provenance, not deployment. Do not deploy them over the
adopted IAM identities or create a second manager for those resources.

[Terraform configuration and adoption guide](../terraform/README.md) is the
current infrastructure entrypoint. Application-owned landing/checkpoint setup
remains in `infra/snowflake/s3_raw_landing.sql` and the ingestion setup code.
