import logging
import pymssql
import random
import datetime
from azure.functions import HttpRequest, HttpResponse

# Database connection configuration
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'


def connect_to_database():
    """Connect to the Azure SQL database."""
    try:
        conn = pymssql.connect(server=server, user=user,
                               password=password, database=database)
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


def main(req: HttpRequest) -> HttpResponse:
    """HTTP trigger to collect data once and generate the chart."""
    logging.info('Python HTTP trigger function processed a request.')

    conn, cursor = connect_to_database()
    if conn and cursor:
        create_table(cursor)
        generate_sensor_data(cursor)
        conn.commit()
        conn.close()
        logging.info("Database connection closed.")
        return HttpResponse("Task1 executed successfully.", status_code=200)
    else:
        return HttpResponse("Failed to connect to the database.", status_code=500)
