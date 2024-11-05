# COMP3211_Distributed_Systems

![Python Badge](https://img.shields.io/badge/Language-Python-blue) ![Azure Badge](https://img.shields.io/badge/Platform-Azure-blue)

## Overview

The project involves creating and deploying a system that uses serverless functions in Microsoft Azure. The solution consists of three primary tasks that demonstrate the use of serverless architecture to collect, process, and automate data management for an IoT network.

## Tasks

### Task 1: Simulated Data Collection
- **Description**: A local HTTP-triggered Azure Function that simulates the collection of environmental data from 20 sensors. The data collected includes sensor ID, temperature, wind speed, humidity, and CO2 levels, which is stored in an Azure SQL database for further processing.
- **Implementation**: Implemented as a Python-based Azure Function that can be triggered locally to test data collection.
- **Key Components**:
  - **Function Type**: HTTP Trigger
  - **Data Storage**: Azure SQL Database (`distributed_systems_deck`)

### Task 2: Data Statistics
- **Description**: A local HTTP-triggered Azure Function designed to analyze data stored in the Azure SQL database. It calculates and outputs statistical information such as the minimum, maximum, and average values for each sensor's data.
- **Implementation**: The function runs locally and connects to the database to perform the analysis.
- **Key Components**:
  - **Function Type**: HTTP Trigger
  - **Analysis Performed**: Computes min, max, and average values for temperature, wind speed, humidity, and CO2 levels for each sensor.

### Task 3: Realistic Scenario Implementation
- **Description**: This task involves deploying a set of serverless functions on Azure that automate data collection and processing. A timer-triggered function collects and inserts data at regular intervals, while an SQL-triggered function automatically processes new data entries to generate statistics.
- **Implementation**: Deployed as an Azure Function App containing two sub-functions:
  - **Timer Trigger (Task 1)**: Automatically simulates data collection at set intervals and stores the data in the database.
  - **SQL Trigger (Task 2)**: Monitors the database for new entries and performs statistical analysis when changes are detected.
- **Key Components**:
  - **Function Types**: Timer Trigger for data collection and SQL Trigger for real-time data processing.
  - **Deployment**: Deployed as an Azure Function App for automated execution.

## Azure Setup

### Services Used:
- **Azure SQL Server**: `deck-server.database.windows.net`
- **Azure SQL Database**: `distributed_systems_deck`
- **Azure Functions**: Included HTTP, Timer and SQL trigger functions.
- **Application Insights**: Used to monitor and log the performance of the functions.

## Folder Structure

```plaintext
COMP3211_Distributed_Systems/
│
├── task1/               # Folder for local Task1
│   ├── host.json
│   ├── local.settings.json
│   ├── task1/           # HTTP Trigger Function
│
├── task2/               # Folder for local Task2
│   ├── host.json
│   ├── local.settings.json
│   ├── task2/           # HTTP Trigger Function
│
└── task3/               # Folder for Azure-deployed Task3 function application
    ├── host.json── 
    ├── local.settings.json # Local configuration (excluded from Git)
    ├── task1/           # Timer Trigger Function
    ├── task2/           # SQL Trigger Function
```

## Author
**Yufei Wang**
