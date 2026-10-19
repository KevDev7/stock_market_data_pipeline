import copy
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.check_terraform_adoption import check_plan

ROOT = Path(__file__).resolve().parents[1]


class AdoptionPlanTest(unittest.TestCase):
    def setUp(self):
        self.plan = {
            "format_version": "1.2", "complete": True, "errored": False,
            "resource_changes": [{"address": "aws_s3_bucket.archive", "mode": "managed",
                                  "change": {"actions": ["no-op"], "importing": {"id": "archive"}}}],
        }

    def test_import_only_plan_passes(self):
        self.assertEqual(check_plan(self.plan, require_imports=True)["imports"], 1)

    def test_final_no_change_plan_passes(self):
        del self.plan["resource_changes"][0]["change"]["importing"]
        self.assertEqual(check_plan(self.plan, no_imports=True)["resource_mutations"], 0)

    def test_every_mutation_is_rejected_including_in_place_and_forget(self):
        for actions in [["create"], ["update"], ["delete"], ["delete", "create"],
                        ["create", "delete"], ["forget"], [], ["read"]]:
            with self.subTest(actions=actions):
                plan = copy.deepcopy(self.plan)
                plan["resource_changes"][0]["change"]["actions"] = actions
                with self.assertRaises(ValueError):
                    check_plan(plan)

    def test_incomplete_errored_deferred_drift_and_unresolved_checks_fail(self):
        for key, value in [("complete", False), ("errored", True), ("deferred_changes", [{}]),
                           ("resource_drift", [{}]), ("checks", [{"status": "unknown"}])]:
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    check_plan(dict(self.plan, **{key: value}))

    def test_missing_complete_and_empty_plans_fail_closed(self):
        del self.plan["complete"]
        with self.assertRaises(ValueError):
            check_plan(self.plan)
        with self.assertRaises(ValueError):
            check_plan({"format_version": "1.2", "complete": True})

    def test_import_expectations(self):
        with self.assertRaises(ValueError):
            check_plan(self.plan, no_imports=True)
        del self.plan["resource_changes"][0]["change"]["importing"]
        with self.assertRaises(ValueError):
            check_plan(self.plan, require_imports=True)

    def test_data_reads_allowed(self):
        self.plan["resource_changes"].append({"mode": "data", "change": {"actions": ["read"]}})
        check_plan(self.plan)


class DashboardMetadataImportTest(unittest.TestCase):
    def setUp(self):
        before = dict(name="STREAMLIT_DASHBOARD_USER", login_name="STREAMLIT_DASHBOARD_USER",
                      disabled="false", default_role="STREAMLIT_DASHBOARD_ROLE",
                      rsa_public_key="fake-public-key", mins_to_unlock=None,
                      parameters=[{"timezone": [{"value": "UTC"}]}],
                      show_output=[{"name": "STREAMLIT_DASHBOARD_USER"}])
        after = dict(before, show_output=None)
        self.plan = {
            "format_version": "1.2", "complete": True, "errored": False,
            "resource_changes": [{
                "address": 'snowflake_service_user.project["dashboard"]',
                "type": "snowflake_service_user", "mode": "managed",
                "provider_name": "registry.terraform.io/snowflakedb/snowflake",
                "change": {"actions": ["update"], "before": before, "after": after,
                           "after_unknown": {"show_output": True, "parameters": [{"timezone": [{}]}]},
                           "importing": {"id": '"STREAMLIT_DASHBOARD_USER"'}},
            }],
        }

    def check(self, plan=None):
        return check_plan(plan or self.plan, require_imports=True,
                          allow_dashboard_user_metadata_refresh=True)

    def test_explicit_exception_accepts_only_metadata_and_reports_update(self):
        with self.assertRaises(ValueError):
            check_plan(self.plan, require_imports=True)
        result = self.check()
        self.assertEqual(result, {"managed_resources": 1, "imports": 1,
                                  "resource_mutations": 1, "permitted_metadata_updates": 1})

    def test_wrong_resource_identity_provider_or_import_rejected(self):
        for key, value in [
            ("address", 'snowflake_service_user.project["other"]'),
            ("type", "snowflake_user"), ("provider_name", "other/provider"),
            ("mode", "data"),
        ]:
            with self.subTest(key=key):
                plan = copy.deepcopy(self.plan)
                plan["resource_changes"][0][key] = value
                with self.assertRaises(ValueError):
                    self.check(plan)
        for importing in [None, {}, {"id": '"OTHER_USER"'}, {"id": "STREAMLIT_DASHBOARD_USER"}]:
            with self.subTest(importing=importing):
                plan = copy.deepcopy(self.plan)
                plan["resource_changes"][0]["change"]["importing"] = importing
                with self.assertRaises(ValueError):
                    self.check(plan)

    def test_all_setting_changes_and_unknowns_rejected(self):
        for field in ["name", "login_name", "disabled", "default_role", "default_warehouse",
                      "default_namespace", "default_secondary_roles_option", "comment", "email",
                      "rsa_public_key", "rsa_public_key_2", "mins_to_unlock", "parameters"]:
            for kind in ["changed", "unknown"]:
                with self.subTest(field=field, kind=kind):
                    plan = copy.deepcopy(self.plan)
                    change = plan["resource_changes"][0]["change"]
                    if kind == "changed":
                        change["after"][field] = "different"
                    else:
                        change["after_unknown"][field] = True
                    with self.assertRaises(ValueError):
                        self.check(plan)

    def test_replacement_metadata_shape_or_missing_snapshots_rejected(self):
        for key, value in [("actions", ["delete", "create"]), ("replace_paths", [["name"]]),
                           ("before", None), ("after", None), ("after_unknown", {}),
                           ("after_unknown", {"show_output": True, "name": "invalid"})]:
            with self.subTest(key=key):
                plan = copy.deepcopy(self.plan)
                plan["resource_changes"][0]["change"][key] = value
                with self.assertRaises(ValueError):
                    self.check(plan)

    def test_additional_mutations_or_imports_and_duplicate_exception_rejected(self):
        for extra in [
            {"mode": "managed", "change": {"actions": ["update"]}},
            {"mode": "managed", "change": {"actions": ["no-op"], "importing": {"id": "other"}}},
            self.plan["resource_changes"][0],
        ]:
            plan = copy.deepcopy(self.plan)
            plan["resource_changes"].append(copy.deepcopy(extra))
            with self.assertRaises(ValueError):
                self.check(plan)

    def test_exception_never_bypasses_drift_errors_or_unresolved_checks(self):
        for key, value in [("complete", False), ("errored", True), ("deferred_changes", [{}]),
                           ("resource_drift", [{}]), ("checks", [{"status": "unknown"}])]:
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    self.check(dict(self.plan, **{key: value}))

    def test_final_plan_is_strict_and_exception_cannot_be_reused(self):
        plan = copy.deepcopy(self.plan)
        change = plan["resource_changes"][0]["change"]
        change.pop("importing")
        change.update(actions=["no-op"], after=change["before"], after_unknown={})
        self.assertEqual(check_plan(plan, no_imports=True)["resource_mutations"], 0)
        with self.assertRaises(ValueError):
            self.check(plan)


class InfrastructureBoundaryTest(unittest.TestCase):
    def test_no_table_access_key_or_ownership_resources(self):
        text = "\n".join(p.read_text() for p in (ROOT / "infra/terraform").glob("**/*.tf"))
        for kind in ["snowflake_table", "snowflake_view", "snowflake_grant_ownership", "aws_iam_access_key"]:
            self.assertNotIn('resource "' + kind + '"', text)
        self.assertNotIn('file(var.private_key', text)

    def test_all_managed_resources_have_destruction_protection(self):
        # Bootstrap's subsidiary resources may be removed/reconfigured only there;
        # all adopted workload resources must explicitly prevent destruction.
        import re
        for root in ["aws", "snowflake"]:
            text = (ROOT / "infra/terraform" / root / "main.tf").read_text()
            blocks = re.split(r'(?m)^resource ', text)[1:]
            for block in blocks:
                with self.subTest(resource=block.splitlines()[0]):
                    self.assertIn("prevent_destroy = true", block)

    def test_state_secrets_plans_are_ignored_but_examples_and_locks_are_not(self):
        ignored = ["infra/terraform/aws/terraform.tfstate", "infra/terraform/aws/adopt.tfplan",
                   "infra/terraform/aws/terraform.tfvars", "infra/terraform/aws/current.tfvars.json",
                   "infra/terraform/aws/project.backend.hcl", "infra/terraform/bootstrap/backend.tf",
                   "infra/terraform/aws/.terraform/terraform.tfstate"]
        for path in ignored:
            self.assertEqual(subprocess.run(["git", "check-ignore", "-q", path], cwd=ROOT).returncode, 0, path)
        for path in ["infra/terraform/aws/.terraform.lock.hcl", "infra/terraform/backend.hcl.example"]:
            self.assertNotEqual(subprocess.run(["git", "check-ignore", "-q", path], cwd=ROOT).returncode, 0, path)

    def test_private_inventory_inputs_are_mode_0600_and_never_overwritten(self):
        from scripts.inspect_infrastructure import write_private_variables
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'current.tfvars.json'
            write_private_variables(path, {'private_fixture': 'not-a-real-secret'})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                write_private_variables(path, {})

    def test_private_inventory_refuses_unignored_output_extensions(self):
        from scripts.inspect_infrastructure import write_private_variables
        with self.assertRaises(ValueError):
            write_private_variables('current.json', {})

    def test_invalid_json_does_not_echo_input(self):
        import sys
        result = subprocess.run([sys.executable, '-m', 'scripts.check_terraform_adoption'], cwd=ROOT,
                                input='PRIVATE-TEST-FIXTURE-invalid-json', text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('PRIVATE-TEST-FIXTURE', result.stdout + result.stderr)

    def test_service_user_management_requires_explicit_opt_in(self):
        variables = (ROOT / 'infra/terraform/snowflake/variables.tf').read_text()
        self.assertRegex(variables, r'variable "manage_service_users" \{[^}]*default\s*=\s*false')
        source = (ROOT / 'infra/terraform/snowflake/main.tf').read_text()
        self.assertIn('var.manage_service_users ? var.service_users : {}', source)

    def test_landing_sql_has_no_infrastructure_owner_overlap(self):
        sql = (ROOT / 'infra/snowflake/s3_raw_landing.sql').read_text()
        for statement in ['CREATE STORAGE INTEGRATION', 'CREATE STAGE', 'CREATE FILE FORMAT',
                          'CREATE SCHEMA IF NOT EXISTS RAW', 'USE ROLE ACCOUNTADMIN']:
            self.assertNotIn(statement, sql)
        self.assertIn('CREATE TABLE IF NOT EXISTS RAW.DAILY_STOCKS_RAW', sql)


if __name__ == "__main__":
    unittest.main()
