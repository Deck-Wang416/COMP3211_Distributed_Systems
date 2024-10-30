import pymssql
from datetime import datetime

# 数据库连接配置
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'

def log_to_file(message):
    """将日志写入文件 /tmp/task2_log.txt"""
    with open("/tmp/task2_log.txt", "a") as log_file:
        log_file.write(f"{datetime.now()} - {message}\n")

try:
    # 连接到 Azure SQL
    conn = pymssql.connect(server=server, user=user, password=password, database=database)
    cursor = conn.cursor()
    print("成功连接到数据库！")

    # 查询每个传感器的统计数据
    query = """
    SELECT 
        sensor_id, 
        MIN(temperature) AS min_temp,
        MAX(temperature) AS max_temp,
        AVG(temperature) AS avg_temp,
        MIN(wind_speed) AS min_wind,
        MAX(wind_speed) AS max_wind,
        AVG(wind_speed) AS avg_wind,
        MIN(humidity) AS min_humidity,
        MAX(humidity) AS max_humidity,
        AVG(humidity) AS avg_humidity,
        MIN(co2_level) AS min_co2,
        MAX(co2_level) AS max_co2,
        AVG(co2_level) AS avg_co2
    FROM sensor_data
    GROUP BY sensor_id;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    # 输出统计结果并写入日志文件
    log_to_file("传感器数据统计：")
    for row in results:
        log_to_file(f"Sensor ID: {row[0]}")
        log_to_file(f"  Temperature - Min: {row[1]}, Max: {row[2]}, Avg: {row[3]:.2f}")
        log_to_file(f"  Wind Speed  - Min: {row[4]}, Max: {row[5]}, Avg: {row[6]:.2f}")
        log_to_file(f"  Humidity    - Min: {row[7]}, Max: {row[8]}, Avg: {row[9]:.2f}")
        log_to_file(f"  CO2 Level   - Min: {row[10]}, Max: {row[11]}, Avg: {row[12]:.2f}")
        log_to_file("-" * 30)

except pymssql.OperationalError as e:
    log_to_file(f"数据库连接失败: {e}")

finally:
    # 关闭数据库连接
    if 'conn' in locals() and conn:
        conn.close()
        log_to_file("数据库连接已关闭。")
