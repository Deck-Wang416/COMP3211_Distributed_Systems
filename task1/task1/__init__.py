import logging
import pymssql
import random
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from time import time
from azure.functions import HttpRequest, HttpResponse

# Database connection configuration
server = 'deck-server.database.windows.net'
user = 'deck_wang'
password = '20030416Wyf.'
database = 'distributed_systems_deck'

# Workloads (number of records to insert in each run)
workloads = [20, 40, 60, 80, 100]
insert_time_records = []

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

def generate_sensor_data(cursor, num_records):
    """Generate and insert a specified number of simulated sensor data records, tracking insert time."""
    start_time = time()
    for i in range(num_records):
        sensor_id = (i % 20) + 1
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
    end_time = time()
    insert_time = end_time - start_time
    insert_time_records.append(insert_time)
    logging.info(f"Inserted {num_records} sensor data records in {insert_time:.4f} seconds.")

def visualize_performance():
    """Plot the performance graph for varying workloads."""
    average_time = sum(insert_time_records) / len(insert_time_records)
    
    plt.figure(figsize=(8, 6))
    plt.plot(workloads, insert_time_records, marker='o', label="Insert Time (s) for Workload")
    plt.axhline(y=average_time, color='r', linestyle='--', label=f"Average Insert Time = {average_time:.4f} s")
    
    plt.xlabel("Workload (Number of Records Inserted)")
    plt.ylabel("Insert Time (s)")
    plt.title("Database Insert Time vs Workload Size")
    plt.legend()
    plt.grid(True)
    plt.savefig("/Users/wang/Desktop/performance_chart.png")
    logging.info("Performance chart saved successfully.")

def main(req: HttpRequest) -> HttpResponse:
    """HTTP trigger to run multiple data insertions and generate performance graph."""
    logging.info('Python HTTP trigger function processed a request.')

    conn, cursor = connect_to_database()
    if conn and cursor:
        create_table(cursor)
        
        for workload in workloads:
            generate_sensor_data(cursor, workload)
            conn.commit()
        
        visualize_performance()
        conn.close()
        logging.info("Database connection closed.")
        
        return HttpResponse("Task1 executed successfully with performance analysis.", status_code=200)
    else:
        return HttpResponse("Failed to connect to the database.", status_code=500)
