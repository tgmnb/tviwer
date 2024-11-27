import os
import polars as pl
from core.utils.log_kit import logger, divider
from core.utils.path_kit import get_file_path, get_folder_path, get_folder_by_root
from config import periods

def swap_data(df,period):
    # print(df['symbol'],df.columns)

    # required_columns = [
    #     "fundingRate", "funding_rate_raw", "funding_rate_r"
    # ]

    # for col in required_columns:
    #     if col not in df.columns:
    #         df = df.with_columns(pl.lit(None).alias(col))
    # df = df.with_columns([
    #     pl.col("fundingRate").cast(pl.Float32, strict=False),
    #     pl.col("funding_rate_raw").cast(pl.Float32, strict=False),
    #     pl.col("funding_rate_r").cast(pl.Float32, strict=False)
    # ])
    aggregated_df = df.group_by_dynamic("candle_begin_time", every=period).agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.col("quote_volume").sum().alias("quote_volume"),
            pl.col("trade_num").sum().alias("trade_num"),
            pl.col("taker_buy_base_asset_volume").sum().alias("taker_buy_base_asset_volume"),
            pl.col("taker_buy_quote_asset_volume").sum().alias("taker_buy_quote_asset_volume"),
            pl.col("avg_price").mean().alias("avg_price"),
            pl.col("symbol").first().alias("symbol"),
            pl.col("fundingRate").first().alias("fundingRate"),
            pl.col("funding_rate_raw").first().alias("funding_rate_raw"),
            pl.col("funding_rate_r").first().alias("funding_rate_r")
                ])
    return aggregated_df

def spot_data(df,period):
    aggregated_df = df.group_by_dynamic("candle_begin_time", every=period).agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.col("quote_volume").sum().alias("quote_volume"),
            pl.col("trade_num").sum().alias("trade_num"),
            pl.col("taker_buy_base_asset_volume").sum().alias("taker_buy_base_asset_volume"),
            pl.col("taker_buy_quote_asset_volume").sum().alias("taker_buy_quote_asset_volume"),
            pl.col("avg_price").mean().alias("avg_price"),
            pl.col("symbol").first().alias("symbol")
            ])
    return aggregated_df

def aggregate_data(input_dir, output_dir, periods,type):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for period in periods:
        period_data = {}
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                symbol = file.split('.')[0]
                file_path = os.path.join(root, file)
                df = pl.read_ipc(file_path,memory_map=False)
                df = df.with_columns(pl.col("candle_begin_time").cast(pl.Datetime))
                df = df.set_sorted("candle_begin_time")
                if type == 'spot':
                    aggregated_df = spot_data(df,period)
                elif type == 'swap':
                    aggregated_df = swap_data(df,period)
                period_data[symbol] = aggregated_df
        dataframes = []
        for symbol, df in period_data.items():
            df = df.with_columns(pl.lit(symbol).alias("symbol"))
            dataframes.append(df)
        combined_df = pl.concat(dataframes)
        output_file = os.path.join(output_dir, f'{period}.arrow')
        combined_df.write_ipc(output_file,compression=None)
        logger.info(f'【{type}】{period} 数据已保存到 {output_file}')
    logger.ok(f'{type}数据重采样完成')

if __name__ == '__main__':
    try:
        divider('开始重采样数据')
        aggregate_data(get_folder_by_root('market', 'pickle_data_15min', 'swap'), get_folder_by_root('market', 'pickle_fin', 'swap'), periods, 'swap')
        aggregate_data(get_folder_by_root('market', 'pickle_data_15min', 'spot'), get_folder_by_root('market', 'pickle_fin', 'spot'), periods, 'spot')
    except Exception as e:
        print(e)
        logger.error(f"ERROR :resample data {str(e)}")