import logging
import pymssql
from azure.functions import HttpRequest, HttpResponse

# Database connection configuration
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'


def connect_to_database():
    """Connect to the Azure SQL Database."""
    try:
        conn = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = conn.cursor()
        logging.info("Successfully connected to the database!")
        return conn, cursor
    except pymssql.OperationalError as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None, None


def calculate_statistics(cursor):
    """Calculate the minimum, maximum, and average for each sensor."""
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
    GROUP BY sensor_id
    ORDER BY sensor_id;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    statistics_output = ""
    for row in results:
        statistics_output += (
            f"Sensor ID: {row[0]}\n"
            f"  Temperature - Min: {row[1]}, Max: {row[2]}, Avg: {row[3]:.2f}\n"
            f"  Wind Speed  - Min: {row[4]}, Max: {row[5]}, Avg: {row[6]:.2f}\n"
            f"  Humidity    - Min: {row[7]}, Max: {row[8]}, Avg: {row[9]:.2f}\n"
            f"  CO2 Level   - Min: {row[10]}, Max: {row[11]}, Avg: {row[12]:.2f}\n"
            f"{'-' * 30}\n"
        )
    return statistics_output


def main(req: HttpRequest) -> HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    conn, cursor = connect_to_database()
    if conn and cursor:
        statistics = calculate_statistics(cursor)
        conn.close()
        logging.info("Database connection has been closed.")
        return HttpResponse(statistics, status_code=200)
    else:
        return HttpResponse("Failed to connect to the database.", status_code=500)
