import subprocess
import schedule
import time
import subprocess

# 定义运行脚本的函数
def run_script(script_name):
    print("开始运行脚本...")
    # 使用 subprocess.run 运行另一个 Python 脚本
    starttime = time.time()
    result = subprocess.run(["python", script_name], capture_output=True, text=True)
    # 打印脚本输出
    endtime = time.time()
    runtime = endtime - starttime
    print("脚本运行完成，输出如下：,共耗时：", runtime)
    print(result.stdout)
    print(result.stderr)

    # 运行脚本后的其他代码
    print("运行结束后的其他逻辑代码执行...")

# 定时任务：每隔 1 小时运行一次

def run_loop():
        
    schedule.run_pending()
    time.sleep(1)

def define_schedule():
    schedule.every().day.at("15:24").do(run_script("data_center.py"))

    schedule.every(2).hours.do(run_script)
     
def main():
    define_schedule()
    # 持续运行调度器

    print("调度器已启动...")

    while True:
        try:
            run_loop()
        except Exception as err:
            msg = 'ERROR:系统出错，10s之后重新运行，出错原因: ' + str(err)
            print(msg)
            time.sleep(10)
            continue

if __name__ == '__main__':
    main()
