import numbers
from collections.abc import Collection
import polars as pl


def is_scalar(obj):
    return isinstance(obj, (str, bytes)) or not isinstance(obj, (Collection, pl.DataFrame, pl.Series))

def is_numeric(obj):
    return isinstance(obj, numbers.Number)


def dataframe_divide(numerator, denominator, group_column, value_column):
    if isinstance(numerator, pl.DataFrame) and isinstance(denominator, pl.DataFrame):
        num_group = f'{group_column}_numerator'
        denom_group = f'{group_column}_denominator'
        num_df = numerator.rename({group_column: num_group, value_column: 'numerator'})
        denom_df = denominator.rename({group_column: denom_group, value_column: 'denominator'})
        operation_df = (
            num_df
            .join(denom_df, how='full', left_on=num_group, right_on=denom_group)
            .with_columns(pl.coalesce([num_group, denom_group]).alias(group_column))
        )

    elif isinstance(numerator, pl.DataFrame) and is_numeric(denominator):
        operation_df = (
                           numerator
                           .rename({value_column: 'numerator'})
                           .with_columns(pl.lit(denominator).alias('denominator'))
        )
    elif is_numeric(numerator) and isinstance(denominator, pl.DataFrame):
        operation_df = (
            denominator
            .rename({value_column: 'denominator'})
            .with_columns(pl.lit(numerator).alias('numerator'))
        )
    elif is_numeric(numerator) and is_numeric(denominator):
        return numerator / denominator if denominator != 0 else None
    else:
        return None
    return operation_df.with_columns(
        (
            pl.when(pl.col('denominator').is_not_null() & (pl.col('denominator') != 0))
            .then(pl.col('numerator') / pl.col('denominator'))
            .otherwise(None)
            .alias(value_column)
        )
    ).select([group_column, value_column])


def dataframe_znorm(dataframe, group_column, value_column):
    temp_column = f'{value_column}_norm'
    return (
        dataframe
        .with_columns(
            (
                    (pl.col(value_column) - pl.col(value_column).mean()) / pl.col(value_column).std()
            ).alias(temp_column)
        )
        .select([group_column, temp_column])
        .rename({temp_column: value_column})
    )
