# 数据重采样周期
periods = ['30m', '1h', '4h', '12h', '1d', '3d', '1w', '1mo','3mo']

# 指标计算列表
factor_list = ['cross_all_market_ave']
DEBUG = False
if DEBUG:
    periods = ['30m']
    factor_list = ['cross_all_market_ave']



idx_list={
    "cross_all_market_ave": {
        "n": 20,
        "factor_name": "cross_all_market_ave"
    },
}