import os

os.environ["http_proxy"] = os.getenv("http_proxy", "http://127.0.0.1:7890")
os.environ["B_CONCURRENCY"] = os.getenv("B_CONCURRENCY", "4")

# 数据重采样周期
periods = ['30m', '1h', '4h', '12h', '1d', '3d', '1w', '1mo','3mo']

# 指标计算列表
factor_list = ['cross_all_market_ave','BTC','cross_all_market_uprate','cross_Altseason']
DEBUG = False
if DEBUG:
    periods = ['30m', '1h', '4h', '12h', '1d', '3d', '1w', '1mo','3mo']

    factor_list = ['cross_Altseason']


idx_list={
    "cross_all_market_ave": {
        "n": 20,
        "factor_name": "cross_all_market_ave"
    },
}