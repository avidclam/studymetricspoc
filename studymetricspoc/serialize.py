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
    for op in results:
        data_item = {}
        for k, v in op.items():
            if k in ['metric', 'level', 'result']:
                data_item[k] = v
            elif k == 'parameter':
                for pk, pv in v.items():
                    data_item[pk] = pv
            elif k == 'scope':
                data_item['scope'] = v.get('name', 'Unknown')
        yield data_item

