import time
import pymssql
import random
import datetime
import matplotlib.pyplot as plt

# 数据库连接配置
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'

def connect_to_database():
    """连接到 Azure SQL 数据库。"""
    try:
        conn = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = conn.cursor()
        print("成功连接到数据库！")
        return conn, cursor
    except pymssql.OperationalError as e:
        print(f"数据库连接失败: {e}")
        return None, None

def create_table(cursor):
    """创建表（如果不存在）。"""
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
    print("数据库表已准备好。")

def generate_sensor_data(cursor):
    """生成并插入模拟传感器数据。"""
    for i in range(20):
        sensor_id = i + 1
        temperature = round(random.uniform(8, 15), 2)
        wind_speed = round(random.uniform(15, 25), 2)
        humidity = round(random.uniform(40, 70), 2)
        co2_level = random.randint(500, 1500)
        timestamp = datetime.datetime.now()

        cursor.execute(
            "INSERT INTO sensor_data (sensor_id, temperature, wind_speed, humidity, co2_level, timestamp) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (sensor_id, temperature, wind_speed, humidity, co2_level, timestamp)
        )
    print("传感器数据已成功插入。")

def visualize_data(cursor):
    """查询数据并生成图表。"""
    cursor.execute("SELECT sensor_id, temperature, wind_speed, humidity, co2_level FROM sensor_data")
    data = cursor.fetchall()

    sensor_ids = [row[0] for row in data]
    temperatures = [row[1] for row in data]
    wind_speeds = [row[2] for row in data]
    humidities = [row[3] for row in data]
    co2_levels = [row[4] for row in data]

    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    plt.plot(sensor_ids, temperatures, marker='o', label='Temperature (°C)', color='r')
    plt.plot(sensor_ids, humidities, marker='x', label='Humidity (%)', color='b')
    plt.xlabel('Sensor ID')
    plt.ylabel('Value')
    plt.title('Temperature and Humidity')
    plt.legend()
    plt.grid(True)

    plt.subplot(1, 2, 2)
    plt.plot(sensor_ids, wind_speeds, marker='s', label='Wind Speed (mph)', color='g')
    plt.plot(sensor_ids, co2_levels, marker='d', label='CO2 Level (ppm)', color='m')
    plt.xlabel('Sensor ID')
    plt.ylabel('Value')
    plt.title('Wind Speed and CO2 Level')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.savefig('/tmp/chart.png')
    print("图表已保存为 /tmp/chart.png")

def main():
    """主程序，定时采集数据。"""
    conn, cursor = connect_to_database()
    if conn and cursor:
        create_table(cursor)

        try:
            while True:
                generate_sensor_data(cursor)
                conn.commit()
                print("等待 5 秒后再次插入数据...")
                time.sleep(5)  # 每隔 5 秒运行一次

        except KeyboardInterrupt:
            print("手动停止数据采集。")

        finally:
            visualize_data(cursor)
            conn.close()
            print("数据库连接已关闭。")

if __name__ == "__main__":
    main()
