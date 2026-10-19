"""Reject resource mutations, except an explicitly audited user metadata refresh.

Usage: terraform show -json saved.tfplan | python -m scripts.check_terraform_adoption --require-imports
This validates a plan, not credentials or the completeness of the resource inventory.
Apply only the exact reviewed binary plan, never a new unreviewed plan.
The optional metadata exception validates plan shape, not emitted SQL; verify
query history and effective user settings after adoption (see the Terraform guide).
"""
import argparse
import json
import sys


def _has_unknown(value):
    """Terraform's unknown mask contains booleans and nested containers only."""
    if isinstance(value, dict):
        return any(_has_unknown(item) for item in value.values())
    if isinstance(value, list):
        return any(_has_unknown(item) for item in value)
    if isinstance(value, bool):
        return value
    raise ValueError("Malformed Terraform unknown-value mask")


def _dashboard_metadata_import(resource):
    """Match only the reviewed provider 2.22.0 computed-output import quirk."""
    change = resource.get("change", {})
    before, after = change.get("before"), change.get("after")
    unknown = change.get("after_unknown", {})
    return (
        resource.get("address") == 'snowflake_service_user.project["dashboard"]'
        and resource.get("type") == "snowflake_service_user"
        and resource.get("provider_name") == "registry.terraform.io/snowflakedb/snowflake"
        and resource.get("mode") == "managed"
        and change.get("actions") == ["update"]
        and change.get("importing") == {"id": '"STREAMLIT_DASHBOARD_USER"'}
        and not change.get("replace_paths")
        and isinstance(before, dict) and isinstance(after, dict)
        and before.get("name") == after.get("name") == "STREAMLIT_DASHBOARD_USER"
        and isinstance(before.get("show_output"), list)
        and bool(before["show_output"])
        and after.get("show_output") is None
        and isinstance(unknown, dict) and unknown.get("show_output") is True
        and not _has_unknown({key: value for key, value in unknown.items() if key != "show_output"})
        and {key: value for key, value in before.items() if key != "show_output"}
        == {key: value for key, value in after.items() if key != "show_output"}
    )


def check_plan(plan, require_imports=False, no_imports=False,
               allow_dashboard_user_metadata_refresh=False):
    """Return a value-free summary; never echo potentially secret plan contents."""
    if not isinstance(plan, dict) or not str(plan.get("format_version", "")).startswith("1."):
        raise ValueError("Expected Terraform JSON plan format version 1.x")
    if plan.get("errored") or plan.get("complete") is not True:
        raise ValueError("Plan must be complete and must not contain errors")
    if plan.get("deferred_changes"):
        raise ValueError("Deferred changes are not permitted during adoption")
    if plan.get("resource_drift"):
        raise ValueError("Resource drift requires inspection before adoption")
    failed_checks = [c for c in plan.get("checks", []) if c.get("status") != "pass"]
    if failed_checks:
        raise ValueError("Unresolved or failed Terraform checks")
    imports = 0
    resources = 0
    metadata_updates = 0
    for resource in plan.get("resource_changes", []):
        actions = resource.get("change", {}).get("actions")
        if resource.get("mode") == "data":
            allowed = (["no-op"], ["read"])
        elif resource.get("mode") == "managed":
            allowed = (["no-op"],)
            resources += 1
        else:
            raise ValueError("Unrecognized resource mode")
        if actions not in allowed:
            if allow_dashboard_user_metadata_refresh and _dashboard_metadata_import(resource):
                metadata_updates += 1
            else:
                raise ValueError("Resource mutations are not permitted during adoption")
        if resource.get("change", {}).get("importing") is not None:
            imports += 1
    if not resources:
        raise ValueError("Plan contains no managed resources; inspect the target directory")
    if require_imports and not imports:
        raise ValueError("Expected at least one import")
    if no_imports and imports:
        raise ValueError("Expected adoption to be finished; imports remain")
    if allow_dashboard_user_metadata_refresh and (metadata_updates != 1 or imports != 1):
        raise ValueError("Dashboard exception requires exactly one user import and metadata update")
    result = {"managed_resources": resources, "imports": imports, "resource_mutations": metadata_updates}
    if allow_dashboard_user_metadata_refresh:
        result["permitted_metadata_updates"] = metadata_updates
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--require-imports", action="store_true")
    group.add_argument("--no-imports", action="store_true")
    parser.add_argument("--allow-dashboard-user-metadata-refresh", action="store_true",
                        help="One-time reviewed dashboard-user import only; never use for final plans")
    args = parser.parse_args()
    try:
        result = check_plan(json.load(sys.stdin), args.require_imports, args.no_imports,
                            args.allow_dashboard_user_metadata_refresh)
    except (ValueError, TypeError, KeyError) as exc:
        # Do not print decoder messages: they may contain raw state values.
        message = "Invalid JSON plan" if isinstance(exc, json.JSONDecodeError) else str(exc)
        print("REJECTED: " + message, file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
