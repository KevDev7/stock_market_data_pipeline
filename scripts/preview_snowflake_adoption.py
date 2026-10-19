"""Read-only Snowflake import PLAN in a disposable local workspace, never apply.

This is a preview while the S3 backend is unavailable, NOT resource adoption.
Requires initialized providers, existing private *.tfvars.json from inventory,
and the saved Snowflake connection. Nothing is imported into persistent state.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from scripts.check_terraform_adoption import check_plan


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--terraform', default='terraform')
    parser.add_argument('--variables', required=True)
    args = parser.parse_args()
    binary = shutil.which(args.terraform)
    if not binary:
        parser.error('Terraform 1.14.5 is required')
    from src.config import SNOWFLAKE
    key = Path(SNOWFLAKE['private_key_path'])
    if not key.is_file():
        key = ROOT / 'keys' / key.name
    environment = dict(os.environ, TF_LOG='OFF', TF_IN_AUTOMATION='1',
                       SNOWFLAKE_USER=SNOWFLAKE['user'], SNOWFLAKE_AUTHENTICATOR='SNOWFLAKE_JWT',
                       SNOWFLAKE_PRIVATE_KEY=key.read_text())
    environment.pop('SNOWFLAKE_ACCOUNT', None)  # Explicit organization/account inputs take precedence.
    source = ROOT / 'infra/terraform/snowflake'
    with tempfile.TemporaryDirectory(prefix='stock-iac-preview-') as directory:
        work = Path(directory)
        for path in list(source.glob('*.tf')) + [source / '.terraform.lock.hcl']:
            content = path.read_text()
            if path.name == 'versions.tf':
                content, count = re.subn(r'  backend "s3" \{\}\n', '', content)
                if count != 1:
                    raise ValueError('Expected exactly one S3 backend declaration')
            (work / path.name).write_text(content)
        commands = [
            [binary, 'init', '-backend=false', '-input=false', '-lockfile=readonly',
             '-plugin-dir=' + str(source / '.terraform/providers')],
            [binary, 'plan', '-input=false', '-lock=false', '-no-color',
             '-var-file=' + str(Path(args.variables).resolve()), '-out=preview.tfplan'],
        ]
        for command in commands:
            process = subprocess.run(command, cwd=work, env=environment, capture_output=True, text=True)
            if process.returncode:
                # No private input values are placed in command-line arguments.
                # Show addresses and attribute names only, not imported state
                # (Snowflake describe outputs can include external IDs/keys).
                address = None
                for line in process.stdout.splitlines():
                    if line.startswith('  # '):
                        address = line.strip()
                        print(address)
                    match = re.match(r' {6}[~+\-]+\s+([a-z_0-9]+)\s+[={]', line)
                    if match and address:
                        print('    attribute:', match.group(1),
                              '(replacement)' if 'forces replacement' in line else '')
                    detail = re.match(r'\s*[~+\-]*\s*([a-z_0-9]+)\s+=', line)
                    if detail and detail.group(1) in {'refresh_on_create', 'auto_refresh', 'multi_line', 'enable'}:
                        if '->' in line or 'forces replacement' in line:
                            print('    stage option:', line.strip())
                    elif 'forces replacement' in line:
                        print('    replacement field:', line.split('=')[0].strip())
                print(process.stderr)
                return process.returncode
        process = subprocess.run([binary, 'show', '-json', 'preview.tfplan'], cwd=work,
                                 env=environment, capture_output=True, text=True, check=True)
        plan = json.loads(process.stdout)
        try:
            summary = check_plan(plan, require_imports=True)
            print(json.dumps(dict(status='import_only_preview_passed_NOT_APPLIED', **summary)))
            return 0
        except ValueError as exc:
            changes = []
            for resource in plan.get('resource_changes', []):
                change = resource['change']
                if change['actions'] == ['no-op']:
                    continue
                before, after = change.get('before') or {}, change.get('after') or {}
                fields = [key for key in set(before) | set(after)
                          if key not in {'show_output', 'describe_output', 'parameters'}
                          and before.get(key) != after.get(key)]
                changes.append(dict(address=resource['address'], actions=change['actions'], fields=sorted(fields)))
            print(json.dumps(dict(status='preview_rejected_NOT_APPLIED', reason=str(exc), changes=changes)))
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
