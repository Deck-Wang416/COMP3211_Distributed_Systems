import pymssql
import logging
import json
from azure.functions import SqlRow

# Database connection configuration
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'

def main(inputDocument: SqlRow) -> None:
    logging.info('SQL Trigger function received data change.')

    try:
        # Connect to Azure SQL Database
        conn = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = conn.cursor()
        logging.info("Connected to the database successfully!")

        # Query to calculate min, max, and average values for each sensor
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

        # Formatting the output for logging
        for row in results:
            logging.info(
                f"Sensor ID: {row[0]}, "
                f"Temperature - Min: {row[1]}, Max: {row[2]}, Avg: {round(row[3], 2)}, "
                f"Wind Speed - Min: {row[4]}, Max: {row[5]}, Avg: {round(row[6], 2)}, "
                f"Humidity - Min: {row[7]}, Max: {row[8]}, Avg: {round(row[9], 2)}, "
                f"CO2 Level - Min: {row[10]}, Max: {row[11]}, Avg: {round(row[12], 2)}"
            )

        logging.info("Sensor data statistics have been successfully retrieved and logged.")

    except pymssql.OperationalError as e:
        logging.error(f"Database connection failed: {e}")

    finally:
        # Close the database connection
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Database connection has been closed.")
