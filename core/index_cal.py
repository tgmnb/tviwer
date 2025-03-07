from core.utils.factor_hub import FactorHub
from core.utils.log_kit import logger, divider
from core.utils.path_kit import get_file_path, get_folder_path
from concurrent.futures import ProcessPoolExecutor, as_completed
import config as conf
import polars as pl

def get_factor(factor_name):

    return FactorHub.get_by_name(factor_name)

def read_data(type, period):
    """
    读取数据
    """
    if type == 'spot':
        data_path = get_file_path( 'market', 'pickle_fin', 'spot', f'{period}.arrow')
    elif type == 'swap':
        data_path = get_file_path( 'market', 'pickle_fin', 'swap', f'{period}.arrow')
    # return pl.(data_path)
    return pl.read_ipc(data_path, memory_map=True)

def cal_index( multi_process=True, silent=False):
    """
    计算指数
    - is_use_spot: True的时候，使用现货数据和合约数据;
    - False的时候，只使用合约数据。所以这个情况更简单

    """
    if silent:
        import logging
        logger.setLevel(logging.WARNING)  # 可以减少中间输出的log

    result_folder = get_folder_path('data', 'index')
    for type in ['spot', 'swap']:
        logger.info(f'开始计算 {type} 数据')
        for period in conf.periods:
            data = read_data(type, period)

            for factor_name in conf.factor_list:
                factor = get_factor(factor_name)
                factor_name_inside = f'{type}_{factor_name}_{period}'
                result = factor.signal(data, factor_name_inside)
                result.write_ipc(get_file_path('data', 'index',factor_name, f'{factor_name_inside}.arrow'), compression=None)
            logger.ok(f'{type} {period} 数据计算完成')
    

if __name__ == '__main__':
    divider('开始计算指标')
    cal_index()