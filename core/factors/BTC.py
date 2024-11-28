import polars as pl

def signal(*args):
    df = args[0]
    factor_name = args[1]
    df=df.filter(pl.col('symbol')=='BTC-USDT' ).select(pl.col('candle_begin_time'),pl.col('close').alias(factor_name))
    # df=df.select(pl.col('close').alias(factor_name))
    print(df)

    return df
