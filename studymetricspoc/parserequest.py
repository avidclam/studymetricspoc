from itertools import cycle, islice
import polars as pl
from .enginecontext import get_engine_context
from .engine import is_scalar, get_engine_global, filter_scope


def lookup_item(data, local_, global_):
    if isinstance(data, str):
        result = local_.get(data, data)
        if isinstance(data, str) and data.strip().startswith('global:'):
            lookup = data.split(':')[1].strip()
            result = global_.get(lookup)
        result = [result] if is_scalar(result) else result
    else:
        result = data
    return result


def nested_lookup_item(data, local_, global_):
    data = lookup_item(data, local_, global_)
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            value = nested_lookup_item(value, local_, global_)
            result[key] = value
    else:
        result = data
    return result


async def parse_request(request):
    ec = get_engine_context()
    request_default = request.get('$.engine.default', {})
    engine_default = ec.pc.get('ENGINE_DEFAULT', {})
    request_local = request.get('$.engine.local', {})
    engine_global = await get_engine_global()
    batch_oplist = []
    for opgroup in request.get('$.engine.batch', []):
        # Metric
        metric_item = opgroup.get('metric',
                                  request_default.get('metric',
                                                      engine_default.get('metric', [])))
        metric_list = lookup_item(metric_item, request_local, engine_global)
        # Level
        level_item = opgroup.get('level',
                                 request_default.get('level',
                                                     engine_default.get('level', [])))
        level_list = lookup_item(level_item, request_local, engine_global)
        # Scope
        scope_item = opgroup.get('scope',
                                 request_default.get('scope',
                                                     engine_default.get('scope', {})))
        scope_name = scope_item.get('name', 'Unknown')
        scope_dict = nested_lookup_item(scope_item, request_local, engine_global)
        scope_df = filter_scope(scope_dict.get('hierarchy', []), scope_dict.get('where', None))
        scope = {'name': scope_name, 'df': scope_df}
        # Parameter
        parameter_item = opgroup.get('parameter',
                                     request_default.get('parameter',
                                                         engine_default.get('parameter', {})))
        parameter_dict = nested_lookup_item(parameter_item, request_local, engine_global)
        max_len = max(len(lst) for lst in parameter_dict.values())
        parameter_list = pl.DataFrame({
            key: list(islice(cycle(values), max_len))
            for key, values in parameter_dict.items()
        }).to_dicts()
        # Operations
        oplist = []
        for metric in metric_list:
            for level in level_list:
                for parameter in parameter_list:
                        oplist.append({
                            'metric': metric,
                            'level': level,
                            'scope': scope,
                            'parameter': parameter
                        })
        batch_oplist.extend(oplist)
    return batch_oplist