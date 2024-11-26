import subprocess
import schedule
import time
import subprocess
import core.data.data_resample as data_resample
import datetime
from core.utils.log_kit import logger

# 定义运行脚本的函数
def run_script():
    '''
    主要脚本
    启动数据中心脚本，进行数据聚合
    '''
    logger.info("开始运行脚本...")
    # 运行数据中心
    s_time = datetime.datetime.now()
    result = subprocess.run(["python", ".\\core\\data\\data_center.py"], capture_output=True, text=True)
    endtime = time.time()
    logger.info(f"数据中心运行结束,共耗时：{datetime.datetime.now() - s_time}")
    # 运行数据重采样
    result = subprocess.run(["python", ".\\core\\data\\data_resample.py"], capture_output=True, text=True)
    logger.info(f"数据重采样运行结束,共耗时：{datetime.datetime.now() - s_time}")
    # 计算指标
    result = subprocess.run(["python", ".\\core\\index_cal.py"], capture_output=True, text=True)
    logger.info(f"指标计算运行结束,共耗时：{datetime.datetime.now() - s_time}")



def run_loop():
    schedule.run_pending()
    time.sleep(1)

def define_schedule():
    # 运行脚本
    schedule.every().day.at("16:43").do(run_script)
    time.sleep(2)
    schedule.every(2).hours.do(run_script)


def main():
    define_schedule()
    # 持续运行调度器

    logger.info("调度器已启动...")

    while True:
        try:
            run_loop()
        except Exception as err:
            msg = 'ERROR:系统出错，10s之后重新运行，出错原因: ' + str(err)
            logger.info(msg)
            time.sleep(10)
            continue


if __name__ == '__main__':
    main()
