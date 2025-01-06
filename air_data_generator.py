# 导入所需的Python库
import mysql.connector                # MySQL数据库连接库
from mysql.connector import pooling   # MySQL连接池管理
import random                        # 随机数生成
from datetime import datetime, timedelta  # 日期时间处理
from snowflake import SnowflakeGenerator # 雪花ID生成器
from config import DB_CONFIG         # 导入数据库配置
import threading                     # 线程管理
from queue import Queue             # 线程安全的队列
import time                         # 时间处理
import signal                       # 信号处理
import sys                          # 系统相关

# 创建雪花ID生成器实例，用于生成全局唯一的ID
gen = SnowflakeGenerator(42)        # 42是机器ID，范围是0-1023，确保在分布式系统中唯一

# 定义各种空气质量参数的合理取值范围，包括最小值和最大值
PARAMETER_RANGES = {
    'pm25': (0, 500),       # PM2.5细颗粒物浓度范围：0-500 μg/m³
    'pm10': (0, 600),       # PM10可吸入颗粒物浓度范围：0-600 μg/m³
    'co': (0, 15),          # 一氧化碳浓度范围：0-15 mg/m³
    'no2': (0, 200),        # 二氧化氮浓度范围：0-200 μg/m³
    'so2': (0, 500),        # 二氧化硫浓度范围：0-500 μg/m³
    'o3': (0, 300),         # 臭氧浓度范围：0-300 μg/m³
}

# 定义程序运行的关键参数
BATCH_SIZE = 1000           # 每批次处理的数据量，用于批量插入数据库
NUM_THREADS = 4             # 工作线程数量，用于并行处理数据
data_queue = Queue()        # 创建线程安全的队列，用于存储待处理的数据

# 创建线程安全的统计和检查机制
insert_counts = {i: 0 for i in range(NUM_THREADS)}    # 记录每个线程插入的数据量
thread_locks = {i: threading.Lock() for i in range(NUM_THREADS)}    # 为每个线程创建一个锁
last_times = {i: None for i in range(NUM_THREADS)}    # 记录每个线程最后处理的时间

# 添加程序状态控制
running = True              # 控制程序运行状态的标志
connection_pool = None      # 全局连接池对象
active_threads = []         # 活动线程列表

def signal_handler(signum, frame):
    """
    信号处理函数，用于优雅地处理程序终止
    :param signum: 信号编号
    :param frame: 当前栈帧
    """
    global running          # 声明全局变量
    print("\n收到终止信号，正在安全停止程序...")  # 提示用户程序正在停止
    running = False         # 设置运行状态为False
    cleanup_resources()     # 清理资源
    sys.exit(0)            # 退出程序

def cleanup_resources():
    """
    清理程序资源，确保所有资源都被正确释放
    """
    global connection_pool, active_threads    # 声明全局变量
    
    try:
        # 向队列发送终止信号，通知所有工作线程停止
        try:
            # 清空队列中的剩余数据
            while not data_queue.empty():
                try:
                    data_queue.get_nowait()
                except:
                    break
            # 发送终止信号
            data_queue.put(None)
        except:
            pass
        
        # 等待所有工作线程完成当前工作
        if active_threads:
            for t in active_threads:
                if t and t.is_alive():
                    try:
                        t.join(timeout=5)  # 等待最多5秒
                    except:
                        pass
                
        # 关闭所有数据库连接
        if connection_pool:
            try:
                # 获取连接池中的所有连接
                if hasattr(connection_pool, '_cnx_queue'):
                    while not connection_pool._cnx_queue.empty():
                        try:
                            conn = connection_pool._cnx_queue.get_nowait()
                            if conn and conn.is_connected():
                                conn.close()    # 关闭每个连接
                        except:
                            pass
            except:
                pass
            
        print("资源清理完成")       # 提示用户清理完成
        
    except Exception as e:
        print(f"清理资源时发生错误: {str(e)}")  # 输出清理过程中的错误，使用str(e)避免某些异常的格式化问题

def generate_random_value(min_val, max_val, decimals=2):
    """
    生成指定范围内的随机数
    :param min_val: 最小值
    :param max_val: 最大值
    :param decimals: 小数位数
    :return: 随机数
    """
    # 在指定范围内生成随机浮点数
    value = random.uniform(min_val, max_val)
    # 四舍五入到指定小数位
    return round(value, decimals)

def generate_air_quality_data(mn, monitor_time):
    """
    生成一条空气质量数据记录
    :param mn: 设备编号
    :param monitor_time: 监测时间
    :return: 包含所有监测数据的字典
    """
    # 创建一条完整的空气质量数据记录，包含所有必要字段
    data = {
        'id': str(next(gen)),  # 使用雪花算法生成唯一ID，确保全局唯一性
        'mn': mn,              # 设备编号，用于标识不同的监测设备
        'monitor_time': monitor_time,  # 监测时间，记录数据产生的时间点
        # 生成各项空气质量指标的随机值，确保在合理范围内
        'pm25': generate_random_value(*PARAMETER_RANGES['pm25']),    # PM2.5浓度，范围0-500
        'pm10': generate_random_value(*PARAMETER_RANGES['pm10']),    # PM10浓度，范围0-600
        'co': generate_random_value(*PARAMETER_RANGES['co'], 3),     # CO浓度，范围0-15，精确到3位小数
        'no2': generate_random_value(*PARAMETER_RANGES['no2']),      # NO2浓度，范围0-200
        'so2': generate_random_value(*PARAMETER_RANGES['so2']),      # SO2浓度，范围0-500
        'o3': generate_random_value(*PARAMETER_RANGES['o3']),        # O3浓度，范围0-300
        'create_time': datetime.now(),    # 记录创建时间，使用当前系统时间
        'update_time': datetime.now()     # 记录更新时间，初始值与创建时间相同
    }
    return data

def check_time_order(thread_id, batch_data):
    """
    检查批次数据的时间顺序是否正确，确保数据时间的连续性
    :param thread_id: 线程ID，用于标识不同的工作线程
    :param batch_data: 待检查的批次数据列表
    """
    # 使用线程锁确保线程安全，防止并发访问导致的问题
    with thread_locks[thread_id]:
        # 获取该线程上一次处理的最后时间点
        current_last_time = last_times[thread_id]
        
        # 检查批次内部的时间顺序，确保时间递增
        for i in range(1, len(batch_data)):
            # 获取相邻两条记录的时间
            prev_time = batch_data[i-1]['monitor_time']  # 前一条记录的时间
            curr_time = batch_data[i]['monitor_time']    # 当前记录的时间
            # 如果发现时间顺序错误，输出警告信息
            if curr_time < prev_time:
                print(f"警告：线程 {thread_id} 发现时间顺序异常！")
                print(f"前一条记录时间: {prev_time}")
                print(f"当前记录时间: {curr_time}")
        
        # 检查与上一批次的时间顺序，确保批次间的时间连续性
        if current_last_time and batch_data[0]['monitor_time'] < current_last_time:
            # 如果当前批次的开始时间早于上一批次的结束时间，输出警告
            print(f"警告：线程 {thread_id} 批次间时间顺序异常！")
            print(f"上一批次最后时间: {current_last_time}")
            print(f"当前批次开始时间: {batch_data[0]['monitor_time']}")
        
        # 更新该线程的最后处理时间，用于下一次检查
        last_times[thread_id] = batch_data[-1]['monitor_time']

def batch_insert_data(connection_pool, thread_id):
    """
    批量插入数据的工作线程函数，负责从队列获取数据并插入数据库
    :param connection_pool: 数据库连接池，用于获取数据库连接
    :param thread_id: 线程ID，用于标识不同的工作线程
    """
    conn = None    # 数据库连接对象
    cursor = None  # 数据库游标对象
    
    try:
        # 从连接池获取数据库连接
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        
        # 持续处理数据，直到收到停止信号
        while running:  # 检查程序运行状态
            # 用于存储一批待插入的数据
            batch_data = []
            try:
                # 从队列中获取数据，直到达到批处理大小或遇到终止信号
                for _ in range(BATCH_SIZE):
                    try:
                        # 从队列中获取一条数据，设置超时以便定期检查运行状态
                        data = data_queue.get(timeout=1)  # 1秒超时
                    except Queue.Empty:
                        continue  # 队列为空时继续等待
                        
                    # 检查是否收到终止信号
                    if data is None:
                        data_queue.put(None)  # 传递终止信号给其他线程
                        # 处理剩余的数据
                        if batch_data:
                            check_time_order(thread_id, batch_data)
                            do_batch_insert(cursor, batch_data)
                            conn.commit()  # 提交事务
                        return  # 结束线程
                        
                    # 将数据添加到批处理列表
                    batch_data.append(data)
                    
                    # 如果达到批处理大小，执行插入操作
                    if len(batch_data) >= BATCH_SIZE:
                        check_time_order(thread_id, batch_data)
                        do_batch_insert(cursor, batch_data)
                        conn.commit()  # 提交事务
                        # 更新该线程的插入计数
                        with thread_locks[thread_id]:
                            insert_counts[thread_id] += len(batch_data)
                        batch_data = []  # 清空批处理列表
                    
                # 处理剩余的数据
                if batch_data:
                    check_time_order(thread_id, batch_data)
                    do_batch_insert(cursor, batch_data)
                    conn.commit()  # 提交事务
                    with thread_locks[thread_id]:
                        insert_counts[thread_id] += len(batch_data)
                    
            except mysql.connector.Error as err:
                # 处理数据库操作错误
                print(f"线程 {thread_id} 数据库操作错误: {err}")
                if batch_data:
                    print(f"失败批次大小: {len(batch_data)}")
                # 尝试回滚事务并重新连接
                try:
                    if conn and conn.is_connected():
                        conn.rollback()
                except:
                    pass
                    
    except Exception as e:
        # 处理其他异常
        print(f"线程 {thread_id} 发生错误: {e}")
    finally:
        # 清理资源，确保连接被正确关闭
        try:
            if cursor:
                cursor.close()  # 关闭游标
            if conn and conn.is_connected():
                conn.close()   # 关闭连接
        except Exception as e:
            print(f"线程 {thread_id} 清理资源时发生错误: {e}")

def do_batch_insert(cursor, batch_data):
    """
    执行批量插入操作，将数据写入数据库
    :param cursor: 数据库游标
    :param batch_data: 要插入的数据列表
    """
    # SQL插入语句，包含所有字段
    sql = """
    INSERT INTO air_quality_monitoring 
    (id, mn, monitor_time, pm25, pm10, co, no2, so2, o3, create_time, update_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # 准备批量插入的数据，将字典转换为元组列表
    values = [(d['id'], d['mn'], d['monitor_time'], d['pm25'], d['pm10'], 
              d['co'], d['no2'], d['so2'], d['o3'], d['create_time'], 
              d['update_time']) for d in batch_data]
    # 执行批量插入操作
    cursor.executemany(sql, values)

def get_user_input():
    """
    获取用户输入的参数，包括时间范围、数据间隔和设备数量
    :return: 开始时间，结束时间，时间间隔，设备数量，最大记录数
    """
    print("\n=== 空气质量数据生成器 ===")
    
    # 获取开始时间
    while True:
        try:
            print("\n请输入开始时间")
            # 获取各个时间组件
            start_year = int(input("开始年份 (例如 2024): "))
            start_month = int(input("开始月份 (1-12): "))
            start_day = int(input("开始日期 (1-31): "))
            start_hour = int(input("开始小时 (0-23): "))
            start_minute = int(input("开始分钟 (0-59): "))
            start_second = int(input("开始秒钟 (0-59): "))
            # 创建开始时间对象
            start_time = datetime(start_year, start_month, start_day, 
                                start_hour, start_minute, start_second)
            
            print("\n请输入结束时间")
            # 获取结束时间组件
            end_year = int(input("结束年份 (例如 2024): "))
            end_month = int(input("结束月份 (1-12): "))
            end_day = int(input("结束日期 (1-31): "))
            end_hour = int(input("结束小时 (0-23): "))
            end_minute = int(input("结束分钟 (0-59): "))
            end_second = int(input("结束秒钟 (0-59): "))
            # 创建结束时间对象
            end_time = datetime(end_year, end_month, end_day, 
                              end_hour, end_minute, end_second)
            
            # 验证时间的有效性
            if end_time <= start_time:
                print("\n错误：结束时间必须晚于开始时间！")
                continue
                
            break
        except ValueError as e:
            print("\n错误：请输入有效的时间！")
    
    # 获取时间间隔
    while True:
        print("\n请选择数据生成间隔：")
        print("1. 每秒")
        print("2. 每分钟")
        print("3. 每小时")
        choice = input("请输入选项 (1-3): ")
        if choice in ['1', '2', '3']:
            interval = ['second', 'minute', 'hour'][int(choice)-1]
            break
        print("\n错误：请输入有效的选项！")
    
    # 获取设备数量
    while True:
        try:
            device_count = int(input("\n请输入模拟设备数量 (1-100): "))
            if 1 <= device_count <= 100:
                break
            print("\n错误：设备数量必须在1-100之间！")
        except ValueError:
            print("\n错误：请输入有效的数字！")
    
    # 获取最大记录数限制
    while True:
        try:
            print("\n请选择数据生成限制方式：")
            print("1. 按时间范围生成（无记录数限制）")
            print("2. 设置最大记录数限制")
            limit_choice = input("请输入选项 (1-2): ")
            if limit_choice == '1':
                max_records = None
                break
            elif limit_choice == '2':
                max_records = int(input("请输入最大记录数: "))
                if max_records > 0:
                    break
                print("\n错误：记录数必须大于0！")
            else:
                print("\n错误：请输入有效的选项！")
        except ValueError:
            print("\n错误：请输入有效的数字！")
    
    return start_time, end_time, interval, device_count, max_records

def main():
    """
    主函数，协调整个程序的运行流程
    """
    global connection_pool, active_threads, running
    
    try:
        # 注册信号处理器，处理程序终止信号
        signal.signal(signal.SIGINT, signal_handler)   # 处理 Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # 处理终止信号
        
        # 获取用户输入的参数
        start_time, end_time, interval, device_count, max_records = get_user_input()
        
        # 创建数据库连接池
        print("\n正在初始化数据库连接池...")
        pool_config = DB_CONFIG.copy()
        pool_config['pool_name'] = 'mypool'
        pool_config['pool_size'] = NUM_THREADS
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)
        print("数据库连接池创建成功！")
        
        # 设置时间间隔
        if interval == 'second':
            delta = timedelta(seconds=1)
        elif interval == 'minute':
            delta = timedelta(minutes=1)
        else:
            delta = timedelta(hours=1)

        # 生成设备编号列表
        devices = [f'MN{str(i).zfill(5)}' for i in range(1, device_count + 1)]
        
        # 打印任务信息
        print(f"\n开始生成数据:")
        print(f"开始时间: {start_time}")
        print(f"结束时间: {end_time}")
        print(f"时间间隔: {interval}")
        print(f"设备数量: {device_count}")
        if max_records:
            print(f"最大记录数: {max_records}")
        
        # 启动工作线程
        threads = []
        for i in range(NUM_THREADS):
            # 创建并启动工作线程
            thread_name = f"Worker-{i+1}"  # 创建线程名称
            t = threading.Thread(target=batch_insert_data, 
                               args=(connection_pool, i),
                               name=thread_name)  # 设置线程名称
            t.start()
            threads.append(t)
        
        # 保存活动线程列表
        active_threads = threads
        
        # 生成数据并放入队列
        current_time = start_time
        record_count = 0
        start_process_time = time.time()
        
        try:
            # 主循环：生成数据直到达到结束时间或记录数限制
            while running and current_time <= end_time and (max_records is None or record_count < max_records):
                # 为每个设备生成一条数据
                for mn in devices:
                    # 检查是否需要停止
                    if not running or (max_records is not None and record_count >= max_records):
                        break
                    # 生成并放入队列
                    data = generate_air_quality_data(mn, current_time)
                    data_queue.put(data)
                    record_count += 1
                    
                    # 每生成10000条数据显示一次进度
                    if record_count % 10000 == 0:
                        elapsed_time = time.time() - start_process_time
                        speed = record_count / elapsed_time if elapsed_time > 0 else 0
                        thread_name = threading.current_thread().name
                        print(f"[{thread_name}] 已生成 {record_count} 条数据，"
                              f"当前时间: {current_time}，"
                              f"速度: {speed:.2f} 条/秒")
                        if max_records:
                            print(f"[{thread_name}] 进度: {(record_count/max_records)*100:.2f}%")
                
                # 时间递增
                current_time += delta
                
        except KeyboardInterrupt:
            # 处理用户中断
            print("\n检测到用户中断，正在安全停止...")
            running = False
            
        # 发送终止信号
        data_queue.put(None)
        
        # 等待所有工作线程完成
        for t in threads:
            t.join()
            
        # 计算并显示统计信息
        total_time = time.time() - start_process_time
        print(f"\n数据生成完成！")
        print(f"共生成 {record_count} 条数据")
        print(f"总耗时: {total_time:.2f} 秒")
        print(f"平均速度: {record_count/total_time:.2f} 条/秒")
        
        # 显示每个线程的插入统计
        print("\n各线程插入统计：")
        for thread_id in range(NUM_THREADS):
            thread_name = f"Worker-{thread_id+1}"
            print(f"[{thread_name}] 插入 {insert_counts[thread_id]} 条数据")

    except mysql.connector.Error as err:
        # 处理数据库错误
        thread_name = threading.current_thread().name
        print(f"\n[{thread_name}] 数据库错误: {err}")
    except Exception as e:
        # 处理其他错误
        thread_name = threading.current_thread().name
        print(f"\n[{thread_name}] 发生错误: {e}")
    finally:
        # 清理资源
        cleanup_resources()

# 程序入口点
if __name__ == '__main__':
    main() 