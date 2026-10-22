"""Check a saved naming-migration plan against explicitly reviewed field values."""
import json


COMPUTED = {'show_output','describe_output','parameters','fully_qualified_name'}


def check_rename_plan(plan, expected):
    if (plan.get('complete') is not True or plan.get('errored') or plan.get('resource_drift')
            or plan.get('deferred_changes') or any(c.get('status') != 'pass' for c in plan.get('checks', []))):
        raise ValueError('Incomplete, drifted, or unresolved plan')
    changes = []
    for resource in plan.get('resource_changes', []):
        change = resource['change']
        if change.get('importing'):
            raise ValueError('No imports during naming migration')
        if change['actions'] == ['no-op']:
            continue
        address = resource['address']
        if address not in expected or change['actions'] != ['update'] or change.get('replace_paths'):
            raise ValueError('Unexpected resource mutation: ' + address)
        before,after = change['before'],change['after']
        allowed = set(expected[address])
        computed = COMPUTED | ({'id'} if resource['type'] == 'snowflake_stage_external_s3' else set())
        fields = {k for k in set(before)|set(after) if before.get(k) != after.get(k)}
        if fields - allowed - computed:
            raise ValueError('Unexpected configurable-field change: ' + address)
        for field,value in expected[address].items():
            actual = after.get(field)
            if field == 'policy':
                actual,value = json.loads(actual),json.loads(value)
            if field == 'storage_allowed_locations':
                actual,value = sorted(actual),sorted(value)
            if actual != value:
                raise ValueError('Planned field differs from reviewed value: ' + address + '.' + field)
        def unknown(value):
            if isinstance(value,dict):return any(unknown(v) for v in value.values())
            if isinstance(value,list):return any(unknown(v) for v in value)
            return value is True
        if any(unknown(v) for k,v in change.get('after_unknown',{}).items() if k not in computed):
            raise ValueError('Unknown configurable field: ' + address)
        changes.append(dict(address=address,fields=sorted(fields-computed)))
    return changes
