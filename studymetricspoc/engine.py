import asyncio
import polars as pl
from .enginecontext import get_engine_context
from .dfmethods import is_scalar

def filter_scope(df, where=None):
    result = pl.DataFrame(df).select(pl.all())
    if where is not None:
        for column in result.columns:
            if column in where:
                result = result.filter(pl.col(column).is_in(where[column]))
    return result


async def run_named_query(name, query, conn):
    query_result = await conn.execute(query)
    df = query_result.get_as_pl()
    if df.shape == (1, 1):
        result = df[0, 0]
    elif df.shape[1] == 1:
        result = df[:, 0].to_list()
    else:
        result = df
    return name, result


async def get_engine_global():
    ec = get_engine_context()
    engine_global_query = ec.pc.get('ENGINE_GLOBAL', {})
    tasks = [
        asyncio.create_task(run_named_query(name, query, conn=ec.gconn))
        for name, query in engine_global_query.items()
    ]
    results = await asyncio.gather(*tasks)
    engine_global = dict(results)
    return engine_global