# Smart City Real-Time Data Pipeline

This Respository encompass a smart city real-time data pipeline that integrates diverse data sources, including vehicle information, camera feeds, weather updates, emergency alerts, and GPS data. Using Apache Kafka for data ingestion and Apache Spark for real-time processing, the system operates within a Docker containerized environment. The processed data is stored in Amazon S3 in raw and transformed formats, with AWS Glue handling data cataloging and transformation. For analytics and visualization, the data is loaded into Amazon Redshift and can be accessed through Power BI, Tableau, and Looker. AWS IAM is implemented to ensure secure access management.

## Introduction
The project integrates multiple data sources, including Vehicle Information, GPS data for route information, weather data, Emergency Information, to Mimic real-world urban scenarios.

![Smartcity Architecture](https://github.com/user-attachments/assets/3d55f2d9-963c-43dc-99f0-811d22d17f64)

## Technologies Used
- AWS (S3, Glue, Redshift, Athena)
- Apache Zookeper
- Apache Kafka
- Apache Spark
- Docker

## Data Simulation and Generation

### Vehicle Data
Simulates vehicle movements, including:
- Speed
- Direction
- Make and model
- Fuel type

### GPS Data
Generates GPS data to track real-time vehicle locations, including:
- Latitude
- Longitude
- Speed
- Direction

### Traffic Camera Data
Simulates traffic camera snapshots and metadata, including:
- Camera ID
- Timestamp
- Location
- Snapshot data (Base64 encoded string)

### Weather Data
Generates weather conditions, including:
- Temperature
- Weather condition (e.g., Sunny, Cloudy, Rain, Snow)
- Precipitation
- Wind speed
- Humidity
- Air Quality Index (AQI)

### Emergency Incident Data
Simulates emergency incidents such as accidents, fires, and medical emergencies, including:
- Incident type
- Timestamp
- Location
- Status (Active or Resolved)
- Description

## Data Ingestion and Streaming

### Kafka Integration
Uses Apache Kafka to stream data from various sources in real-time.

### Topic Management
Organizes data into different Kafka topics for efficient handling and processing:
- vehicle_data
- gps_data
- traffic_data
- weather_data
- emergency_data
  
## Data Processing and Transformation
### spark Streaming
- Utilizes Apache Spark for real-time data processing and transformation.
### Incremental Updates
- Processes data incrementally, simulating vehicle movement from London to Birmingham with periodic updates.

## Data Storage

### Raw Storage
Stores raw, unprocessed data in Amazon S3 for:
- Traceability
- Auditing
- Potential reprocessing
### Transformed Storage
Stores processed and cleaned data in a structured format (e.g., Parquet) in Amazon S3 for:
- Efficient querying
- Analysis

## Data Cataloging and Analysis
- Uses AWS Glue crawlers to catalog data in the AWS Glue Data Catalog, making it searchable and accessible. Enables data querying using Amazon Athena and
Amazon Redshift.

## Installation and Setup

### Prerequisites
- Python 3.6+
- Apache Kafka
- Apache Spark
- AWS account with access to S3, Glue, Athena, and Redshift

## Installation
Clone the repository and install the required Python packages:
'''sh
git clone https://github.com/venkatesh-vuyyala/Real-Time-Urban-Analytics.git
cd Real-Time-Urban-Analytics
pip install -r requirements.txt

## Usage

Run the data simulation:
'''sh
python main.py

