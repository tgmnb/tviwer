import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def aggregate_data(input_dir, output_dir, periods):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for period in periods:
        period_data = {}
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                symbol = file.split('.')[0]
                file_path = os.path.join(root, file)
                df = pd.read_feather(file_path)
                df.set_index('candle_begin_time', inplace=True)
                existing_columns = df.columns.intersection([
                        'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
                        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
                        'avg_price', 'symbol', 'fundingRate', 'funding_rate_raw',
                        'funding_rate_r'
                    ])
                aggregated_df = df[existing_columns].resample(period).agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum',
                        'quote_volume': 'sum',
                        'trade_num': 'sum',
                        'taker_buy_base_asset_volume': 'sum',
                        'taker_buy_quote_asset_volume': 'sum',
                        'avg_price': 'mean',
                        'symbol': 'first',
                        'fundingRate': 'first',
                        'funding_rate_raw': 'first',
                        'funding_rate_r': 'first'
                    })
                period_data[symbol] = aggregated_df
        combined_df = pd.concat(period_data.values(), keys=period_data.keys())
        output_file = os.path.join(output_dir, f'{period}.pkl')
        combined_df.to_feather(output_file)
        print(combined_df)
        print(f'{period} 数据已保存到 {output_file}')

if __name__ == '__main__':
    periods = ['30min', '1h', '4h', '12h', '1D', '3D', '1W', '1ME']
    aggregate_data('.\\market\\pickle_data_15min\\swap\\', '.\\market\\pickle_fin\\swap\\', periods)
    aggregate_data('.\\market\\pickle_data_15min\\spot\\', '.\\market\\pickle_fin\\spot\\', periods)