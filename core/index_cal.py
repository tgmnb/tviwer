from core.utils.factor_hub import FactorHub
from core.utils.log_kit import logger
from core.utils.path_kit import get_file_path, get_folder_path
from concurrent.futures import ProcessPoolExecutor, as_completed

def get_factor(factor_name):
    
    return FactorHub.get_by_name(factor_name)

def cal_index( multi_process=True, silent=False):
    """
    计算指数
    - is_use_spot: True的时候，使用现货数据和合约数据;
    - False的时候，只使用合约数据。所以这个情况更简单

    """
    if silent:
        import logging
        logger.setLevel(logging.WARNING)  # 可以减少中间输出的log

    result_folder = 
    if not multi_process:
        for strategy in conf.strategy_list:
            process_strategy(strategy, result_folder)
        return

    # 多进程模式
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_strategy, stg, result_folder) for stg in conf.strategy_list]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.exception(e)
                exit(1)

