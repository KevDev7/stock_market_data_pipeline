"""Strict scoped plan guard for the dataset-folder migration, not general apply."""
import copy

from scripts.check_polygon_rename_plan import check_rename_plan


def check_layout_plan(plan, updates, overview=None):
    remaining = copy.deepcopy(plan)
    creations = []
    resources = []
    for resource in remaining.get('resource_changes', []):
        change = resource['change']
        if change['actions'] != ['create']:
            resources.append(resource)
            continue
        if (overview is None or creations
                or resource['address'] != 'snowflake_stage_external_s3.overview[0]'
                or resource['type'] != 'snowflake_stage_external_s3'
                or change.get('before') is not None or change.get('importing')
                or change.get('replace_paths')):
            raise ValueError('Unreviewed resource creation')
        if change['after'] != overview:
            raise ValueError('Overview stage differs from the explicitly reviewed configuration')
        def unknown(value):
            if isinstance(value,dict): return any(unknown(v) for v in value.values())
            if isinstance(value,list): return any(unknown(v) for v in value)
            return value is True
        allowed = {'id','fully_qualified_name','show_output','describe_output','parameters','cloud'}
        if any(unknown(v) for k,v in change.get('after_unknown',{}).items() if k not in allowed):
            raise ValueError('Unknown overview-stage configuration')
        creations.append(resource['address'])
    if overview is not None and not creations:
        raise ValueError('Expected the reviewed overview-stage creation')
    remaining['resource_changes'] = resources
    changes = check_rename_plan(remaining, updates)
    return dict(updates=changes, creations=creations)
