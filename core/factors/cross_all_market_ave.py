import polars as pl

def signal(*args):
    df = args[0]
    factor_name = args[1]

    # 计算涨跌幅（按 symbol 分组并计算相邻行差分）
    df = (
        df.sort(["symbol", "candle_begin_time"])  # 按币种和时间排序
        .with_columns(
            (pl.col("close").diff() / pl.col("close").shift(1))
            .over("symbol")
            .alias("price_change")  # 计算涨跌幅
        )
    )
    df = (
    df.group_by("candle_begin_time")
      .agg(pl.col("price_change").mean().alias('avg_price_change'))
    )
    # print(average_changes)
    df = df.fill_nan(0)
    # 用平均涨跌幅累乘，计算全市场指数
    df = df.with_columns(
        (1 + pl.col('avg_price_change')).cum_prod().alias(factor_name)
    )
    print(df)
    return df
