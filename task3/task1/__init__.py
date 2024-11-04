import pymssql
import random
import datetime
import logging
from azure.functions import TimerRequest

# Database connection configuration
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'


def main(mytimer: TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        # Connect to Azure SQL Database
        conn = pymssql.connect(server=server, user=user,
                               password=password, database=database)
        cursor = conn.cursor()
        logging.info("Connected to the database successfully!")

        # Create table if it does not exist
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
        logging.info("Database table is ready.")

        # Generate and insert simulated sensor data
        for i in range(20):
            sensor_id = i + 1  # Sensor ID from 1 to 20
            # Temperature range 8-15°C
            temperature = round(random.uniform(8, 15), 2)
            # Wind speed range 15-25 mph
            wind_speed = round(random.uniform(15, 25), 2)
            humidity = round(random.uniform(40, 70),
                             2)  # Humidity range 40%-70%
            # CO2 concentration range 500-1500 ppm
            co2_level = random.randint(500, 1500)
            timestamp = datetime.datetime.now()  # Current time

            # Insert data into the database
            cursor.execute(
                "INSERT INTO sensor_data (sensor_id, temperature, wind_speed, humidity, co2_level, timestamp) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (sensor_id, temperature, wind_speed, humidity, co2_level, timestamp)
            )
        conn.commit()  # Commit the transaction
        logging.info("Sensor data has been successfully inserted.")

    except pymssql.OperationalError as e:
        logging.error(f"Database connection failed: {e}")

    finally:
        # Close the database connection
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Database connection has been closed.")
