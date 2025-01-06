# 空气质量监测数据生成器
这是一个用Python实现的空气设备数据生成工具，你可以用它来生成一堆测试数据，然后给你用来练习Mysql相关的学习

## 功能特点

- 支持多线程并行处理，提高数据生成效率
- 使用雪花算法生成唯一ID
- 支持批量数据插入
- 数据库连接池管理
- 可自定义时间范围和数据生成间隔（秒/分/时）
- 模拟多个监测设备
- 生成符合实际范围的空气质量数据
- 优雅的程序终止处理

## 安装步骤

1. 克隆代码库：
```bash
git clone [repository_url]
cd air-device-data-simulation
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 创建MySQL数据库和表：

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS air_monitor_db
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- 创建表
CREATE TABLE air_quality_monitoring (
    id VARCHAR(64) NOT NULL COMMENT '雪花算法生成的主键ID',
    mn VARCHAR(32) NOT NULL COMMENT '设备唯一编号(MN号)',
    monitor_time DATETIME NOT NULL COMMENT '监测时间,格式:yyyy-MM-dd HH:mm:ss',
    pm25 DECIMAL(10,2) COMMENT 'PM2.5细颗粒物浓度,单位:μg/m³',
    pm10 DECIMAL(10,2) COMMENT 'PM10可吸入颗粒物浓度,单位:μg/m³',
    co DECIMAL(10,3) COMMENT '一氧化碳浓度,单位:mg/m³',
    no2 DECIMAL(10,2) COMMENT '二氧化氮浓度,单位:μg/m³',
    so2 DECIMAL(10,2) COMMENT '二氧化硫浓度,单位:μg/m³',
    o3 DECIMAL(10,2) COMMENT '臭氧浓度,单位:μg/m³',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据入库时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='空气质量监测数据表';
```

4. 配置数据库连接：
   修改 `config.py` 文件中的数据库连接信息：
```python
DB_CONFIG = {
    'host': 'localhost',      # 数据库主机地址
    'user': 'your_username',  # 数据库用户名
    'password': 'your_password', # 数据库密码
    'database': 'air_monitor_db', # 数据库名称
    'port': 3306             # 数据库端口
}
```

## 使用方法

1. 运行程序：
```bash
python air_data_generator.py
```

2. 按提示输入以下信息：
   - 开始时间（年、月、日、时、分、秒）
   - 结束时间（年、月、日、时、分、秒）
   - 数据生成间隔（每秒/每分钟/每小时）
   - 模拟设备数量（1-100）

3. 程序会显示生成进度和统计信息：
   - 已生成数据量
   - 当前处理时间
   - 数据生成速度
   - 各线程处理统计

## 数据范围说明

生成的空气质量数据范围如下：
- PM2.5：0-500 μg/m³
- PM10：0-600 μg/m³
- CO：0-15 mg/m³
- NO2：0-200 μg/m³
- SO2：0-500 μg/m³
- O3：0-300 μg/m³

## 性能优化

- 使用批量插入提高数据库写入效率
- 使用连接池管理数据库连接
- 多线程并行处理
- 数据生成和插入分离
- 定期提交事务避免事务过大

## 故障排除

1. 如果遇到数据库连接错误：
   - 检查数据库服务是否启动
   - 验证数据库连接信息是否正确
   - 确认数据库用户权限

2. 如果程序性能不佳：
   - 调整 `BATCH_SIZE` 大小
   - 调整工作线程数量
   - 确保使用本地数据库
   - 检查数据库配置是否优化
