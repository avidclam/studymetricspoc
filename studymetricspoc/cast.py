import polars as pl


def cast_pl(df):
    result = df
    if isinstance(df, pl.DataFrame):
        if df.shape == (1, 1):
            result = df[0, 0]
        elif df.shape[1] == 1:
            result = df[:, 0].to_list()
    return result