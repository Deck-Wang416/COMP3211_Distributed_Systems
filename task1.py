import pymssql
import random
import datetime

# 数据库连接配置
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'

try:
    # 连接到 Azure SQL
    conn = pymssql.connect(server=server, user=user, password=password, database=database)
    cursor = conn.cursor()
    print("成功连接到数据库！")

    # 创建表（如果不存在）
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='sensor_data' AND xtype='U')
    CREATE TABLE sensor_data (
        id INT IDENTITY(1,1) PRIMARY KEY,
        sensor_id INT,
        temperature FLOAT,
        wind_speed FLOAT,
        humidity FLOAT,
        co2_level INT,
        timestamp DATETIME
    )
    """)
    conn.commit()
    print("数据库表已准备好。")

    # 生成并插入模拟传感器数据
    def generate_sensor_data():
        for i in range(20):
            sensor_id = i + 1  # 传感器ID从1到20
            temperature = round(random.uniform(8, 15), 2)  # 温度范围8-15°C
            wind_speed = round(random.uniform(15, 25), 2)  # 风速范围15-25 mph
            humidity = round(random.uniform(40, 70), 2)  # 湿度范围40%-70%
            co2_level = random.randint(500, 1500)  # CO2浓度范围500-1500 ppm
            timestamp = datetime.datetime.now()  # 当前时间

            # 插入数据到数据库 (使用 %s 作为占位符)
            cursor.execute(
                "INSERT INTO sensor_data (sensor_id, temperature, wind_speed, humidity, co2_level, timestamp) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (sensor_id, temperature, wind_speed, humidity, co2_level, timestamp)
            )
        conn.commit()  # 提交事务
        print("传感器数据已成功插入。")

    # 调用函数生成并存储数据
    generate_sensor_data()

except pymssql.OperationalError as e:
    print(f"数据库连接失败: {e}")

finally:
    # 关闭数据库连接
    if 'conn' in locals() and conn:
        conn.close()
        print("数据库连接已关闭。")
