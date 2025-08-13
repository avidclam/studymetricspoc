import types
import json
from datetime import datetime
import polars as pl


def serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, pl.DataFrame):
        # return obj.to_dict(as_series=False)
        return obj.to_dicts()
    return {'error': f"Type {type(obj)} not serializable"}


def serialize(data):
    if isinstance(data, types.GeneratorType):
        data = list(data)
    return json.dumps(data, default=serializer, indent=2)

def gen_export(results):
    all_parameter_keys = set()
    for op in results:
        all_parameter_keys.update(op.get('parameter', {}).keys())
    for op in results:
        if 'level' in op and 'metric' in op:
            base_record = {
                'scope': op['scope'].get('name', 'Unknown') if 'scope' in op else None,
                'metric': op['metric'],
                'level': op['level']
            }
            level = base_record['level']
            for param_key in all_parameter_keys:
                base_record[param_key] = op.get('parameter', {}).get(param_key)
            df = op.get('result', {'error': 'Missing result'})
            if isinstance(df, pl.DataFrame):
                for record in df.to_dicts():
                    record_dict = {}
                    record_dict.update(base_record)
                    record_dict.update({
                        'level_value': record.get(level),
                        'metric_value': record.get('value'),
                        'error': None
                    })
                    yield record_dict
            else:
                record_dict = {}
                record_dict.update(base_record)
                record_dict.update({
                    'level_value': None,
                    'metric_value': None,
                    'error': df.get('error', 'Unknown error')
                })
                yield record_dict
