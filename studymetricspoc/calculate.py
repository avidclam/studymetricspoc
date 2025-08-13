import re
import polars as pl
from .enginecontext import get_engine_context
from .engine import filter_scope
from .dfmethods import dataframe_divide

async def calculate_query_metric(query, op):
    ec = get_engine_context()
    op_parameter = op.get('parameter', {})
    query_param = [m[1:] for m in re.findall(r"\$\w+", query)]
    query_subst = query
    if query_param:  # Substitute parameters
        metric_parameter = ec.pc.get('METRIC_PARAMETER', {})
        for param in query_param:
            if param in metric_parameter:
                param_type = metric_parameter[param].get('type', 'STRING')
            else:
                param_type = 'STRING'
            if param in op_parameter:
                param_subst = f"CAST('{str(op_parameter[param])}' AS {param_type})"
            else:
                param_subst = f"${param}"
            query_subst = query_subst.replace(f"${param}", param_subst)
    query_result = await ec.gconn.execute(query_subst)
    result = query_result.get_as_pl()
    if 'scope' in op:  # Filter scope
        result = filter_scope(result, op['scope'].get('df'))
    return result


async def calculate_aggregate_metric(aggregate, op):
    base_level = aggregate.get('base')
    target_level = op['level']
    method = aggregate.get('method')
    base_op = {}
    base_op.update(op)
    base_op['level'] = base_level
    base_df = await calculate_metric(base_op)
    if not isinstance(base_df, pl.DataFrame):
        return {'error': 'Error on the base metric'}
    agg_df = op['scope']['df'].select([target_level, base_level]).unique()
    agg_func = getattr(pl.col("value"), method)()
    return (
        base_df
        .join(agg_df, on=base_level)
        .group_by(target_level)
        .agg(agg_func)
    )


async def calculate_call_metric(call, op):
    result = None
    ec = get_engine_context()
    for call_method, call_args in call.items():
        args = {}
        for arg, value in call_args.items():
            if isinstance(value, str):  # Check for metric
                if value in ec.pc.get_dict('METRIC_CATALOG'):
                    dependent_op = {}
                    dependent_op.update(op)
                    dependent_op['metric'] = value
                    args[arg] = await calculate_metric(dependent_op)
                else:
                    args[arg] = value
        error_data = list(filter(lambda x: isinstance(x, dict),  (data for _, data in args.items())))
        if error_data:
            return {'error': 'Error on the call metric'}
        if call_method == 'divide':
            result =  dataframe_divide(args['numerator'], args['denominator'], op.get('level'), 'value')
        else:
            result = {'error': f'Call method "{call_method}" not supported'}
        break
    if result is None:
        result = {'error': 'Error on the call metric'}
    return result


async def calculate_metric(op):
    metric_name = op.get('metric')
    metric_name_quoted = f'"{metric_name}"'
    ec = get_engine_context()
    metric_catalog_path = ec.pc.alias.get('METRIC_CATALOG', '')
    metric_desc_list = ec.pc.get(f'{metric_catalog_path}.[{metric_name_quoted}]')
    metric_desc = metric_desc_list[0] if metric_desc_list else None
    if not isinstance(metric_desc, dict):
        return {'error': f"Unknown metric '{metric_name}'"}
    level = op.get('level')
    query = metric_desc.get('level', {}).get(level, {}).get('query', '').strip()
    aggregate = metric_desc.get('level', {}).get(level, {}).get('aggregate')
    call = metric_desc.get('level', {}).get(level, {}).get('call', metric_desc.get('call'))
    if query:  # Query Type
        result = await calculate_query_metric(query, op)
    elif aggregate:  # Aggregate type
        result = await calculate_aggregate_metric(aggregate, op)
    elif call:
        result = await calculate_call_metric(call, op)
    else:
        result = {'error': 'Unsupported calculation method'}
    return result


async def run_op(op):
    result = await calculate_metric(op)
    op['result'] = result
    return op