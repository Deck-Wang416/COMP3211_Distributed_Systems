import logging
import pymssql
import random
import datetime
import matplotlib.pyplot as plt
from azure.functions import HttpRequest, HttpResponse

# Database connection configuration
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'


def connect_to_database():
    """Connect to the Azure SQL database."""
    try:
        conn = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = conn.cursor()
        logging.info("Successfully connected to the database!")
        return conn, cursor
    except pymssql.OperationalError as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None, None


def create_table(cursor):
    """Create table if it does not exist."""
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
    logging.info("Table is ready.")


def generate_sensor_data(cursor):
    """Generate and insert simulated sensor data."""
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
    logging.info("Sensor data has been successfully inserted.")


def visualize_data(cursor):
    """Query data and generate a chart."""
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
    plt.show()
    logging.info("Chart has been displayed.")


def main(req: HttpRequest) -> HttpResponse:
    """HTTP trigger to collect data once and generate the chart."""
    logging.info('Python HTTP trigger function processed a request.')

    conn, cursor = connect_to_database()
    if conn and cursor:
        create_table(cursor)
        generate_sensor_data(cursor)
        conn.commit()
        visualize_data(cursor)
        conn.close()
        logging.info("Database connection closed.")
        return HttpResponse("Task1 executed successfully.", status_code=200)
    else:
        return HttpResponse("Failed to connect to the database.", status_code=500)
