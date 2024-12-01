import polars as pl

def signal(*args):
    df = args[0]
    factor_name = args[1]
    n = 90

    # 按照 `symbol` 和 `candle_begin_time` 排序
    df = df.sort(["symbol", "candle_begin_time"])

    # 计算山寨指数
    # 1. 计算每个币种的市值
    df = (
        df.sort(["symbol", "candle_begin_time"])  # 按币种和时间排序
        .with_columns(
            (pl.col("quote_volume").rolling_mean(1000,min_periods=1))
            .over("symbol")
            .alias("market_value")  # 计算市值
        )
    )
    
    # 2. 筛选每日市值排名前 50 的币种
    top_50_df = (
        df.filter(pl.col("symbol") != "BTC-USDT")  # 去除比特币
        .sort(["candle_begin_time", "market_value"], descending=True)  # 按市值降序排列
        .group_by("candle_begin_time")  # 按照时间分组
        .head(49)  # 取每个时刻市值前50的币种
        .select(["candle_begin_time", "symbol", "close"])  # 保留时间和币种信息
    )
    
    # 3. 获取比特币的价格变化
    btc_df = (
        df.filter(pl.col("symbol") == "BTC-USDT")
        .sort(["candle_begin_time"])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(n) - 1).alias("btc_return")  # 计算比特币的涨幅
        )
        .select(["candle_begin_time", "symbol", "btc_return"])  # 确保 btc_return 列存在
    )
    
    # 4. 合并山寨币数据和比特币的涨幅数据
    merged_df = top_50_df.join(btc_df, on="candle_begin_time", how="left").sort(["candle_begin_time"])
    
    # 填充NaN值
    merged_df = merged_df.fill_nan(0)

    # 5. 计算每个币种相对比特币的表现
    merged_df = (
        merged_df.with_columns(
            ((pl.col("close") / pl.col("close").shift(n) - 1)-pl.col("btc_return")).alias("coin_return")  # 计算每个币种的涨幅
    ))

    # 计算相对比特币的表现
    merged_df = merged_df.with_columns(
        (pl.col("coin_return") / pl.col("btc_return")).alias("relative_performance")  # 相对比特币的表现
    )
    
    # 6. 计算表现优于比特币的币种数量
    better_than_btc = (merged_df.filter(pl.col("relative_performance") > 1)
                       .group_by("candle_begin_time")
                       .agg(pl.col("symbol").count().alias(factor_name))
                        )
    return better_than_btc
