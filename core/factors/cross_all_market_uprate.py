import polars as pl

def signal(*args):
    df = args[0]
    factor_name = args[1]

    # 计算涨跌幅（按 symbol 分组并计算相邻行差分）
    # df = df.with_columns(pl.col("candle_begin_time").str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M:%S"))

    # 按照 `symbol` 和 `candle_begin_time` 排序
    df = df.sort(["symbol", "candle_begin_time"])

    # 计算每个币种的价格变化情况（上涨或下跌）
    df = df.with_columns(
        (pl.col("close") > pl.col("close").shift(1)).alias("is_up")
    )

    # 现在，统计每个时段上涨的币种数量
    up_count = df.group_by("candle_begin_time").agg([
        pl.col("is_up").sum().alias("up_count"),  # 计算每个时段上涨的币种数量
        pl.count("symbol").alias("total_count")  # 计算每个时段的币种总数
    ])

    # 计算上涨币种数量占总币种数量的占比
    up_count = up_count.with_columns(
        (pl.col("up_count") / pl.col("total_count")).alias(factor_name)
    )
    # print(up_count)
    return up_count
