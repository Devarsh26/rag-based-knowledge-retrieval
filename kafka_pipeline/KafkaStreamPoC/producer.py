from kafka import KafkaProducer
import os
import json
import datetime
import sqlite3
import re
import subprocess
import pandas as pd
from pyspark.sql import SparkSession

# Kafka producer setup
KAFKA_BROKER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda m: json.dumps(m, default=str).encode('utf-8'))
#172.18.0.3

# Kafka Topics
STRUCTURED_TOPIC = 'structured-data-topic'  # For JSON/XML
UNSTRUCTURED_TOPIC = 'unstructured-data-topic'  # For Word/PDF

# File path to the docs
folder_path = os.path.expanduser('~/Documents/University of Washington/Coursework/Capstone/Capstone_Q5/KakfaStreamPOC/Trial files')

# Connect to SQLite
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

# Checking if the folder exists
if not os.path.exists(folder_path):
    print(f"Error: Folder '{folder_path}' not found.")
    exit()

# Initializing PySpark for JSON/XML batch processing
spark = SparkSession.builder \
    .appName("BatchProcessing") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0") \
    .getOrCreate()

# Function to check if structured file (JSON/XML) is unchanged
def structured_file_is_unchanged(file_name, last_modified_timestamp):
    cursor.execute("SELECT last_modified_timestamp FROM structured_data WHERE file_name = ?", (file_name,))
    result = cursor.fetchone()
    if result:
        existing_timestamp = result[0]
        return existing_timestamp == last_modified_timestamp  # Skip if timestamp matches
    return False  # File does not exist, so it must be new

# Function to check if unstructured file (Word/PDF) is unchanged
def unstructured_file_is_unchanged(file_name, last_modified_timestamp):
    cursor.execute("SELECT last_modified_timestamp FROM unstructured_data WHERE file_name = ?", (file_name,))
    result = cursor.fetchone()
    if result:
        existing_timestamp = result[0]
        return existing_timestamp == last_modified_timestamp  # Skip if timestamp matches
    return False  # File does not exist, so it must be new

# Iterate through all files in the folder
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)

    # Skip directories, process only files
    if os.path.isfile(file_path):
        file_extension = os.path.splitext(file_name)[1].lower()

        # Get file metadata
        file_size = os.path.getsize(file_path)
        output = subprocess.check_output(['stat', '-f%B', file_path]).decode().strip()
        creation_timestamp = datetime.datetime.fromtimestamp(int(output)).isoformat()
        last_modified_timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()

        # Check for duplicates and skip unchanged files
        if file_extension in ['.json', '.xml', '.xlsx', '.csv']:
            if structured_file_is_unchanged(file_name, last_modified_timestamp):
                print(f"/ Skipping unchanged structured file: {file_name}")
                continue
        elif file_extension in ['.docx', '.pdf', '.png', '.jpg', '.jpeg']:
            if unstructured_file_is_unchanged(file_name, last_modified_timestamp):
                print(f"/ Skipping unchanged unstructured file: {file_name}")
                continue

        # Process JSON/XML with PySpark
        if file_extension in ['.json', '.xml', '.xlsx', '.csv']:
            df = None
            try:
                if file_extension == ".json":
                    df = spark.read.option("multiline", "true").json(file_path)
                elif file_extension == ".xml":
                    with open(file_path, "r", encoding="utf-8") as xml_file:
                        for line in xml_file:
                            match = re.search(r"<(\w+)>", line.strip())  # Find first valid tag
                            if match:
                                root_tag = match.group(1)
                                break  # Exit loop after finding the first valid XML tag

                    
                    # Extract actual row elements
                    df = spark.read.format("com.databricks.spark.xml").option("rowTag", root_tag[:-1]).load(file_path)  # Fix: Use singular form of root tag

                elif file_extension == ".xlsx":
                    # Read all sheets from Excel file
                    xls = pd.read_excel(file_path, sheet_name=None)
                    
                    # Structure the data by sheet
                    sheet_data = {}
                    for sheet_name, df in xls.items():
                        # Handle NaN values and convert to records
                        records = df.fillna("").to_dict(orient="records")
                        # Include sheet structure 
                        sheet_data[sheet_name] = records
                    
                    kafka_message = {
                        "file_name": file_name,
                        "file_path": file_path,
                        "file_type": "excel",
                        "data": sheet_data,  # Sheet wise data
                        "creation_timestamp": creation_timestamp,
                        "last_modified_timestamp": last_modified_timestamp
                    }
                    producer.send(STRUCTURED_TOPIC, value=kafka_message)
                    producer.flush()
                    print(f"+ Sent Excel file data from {file_name} to {STRUCTURED_TOPIC}.")
                    continue

                elif file_extension == ".csv":
                    # Read CSV into pandas Dataframe
                    df = pd.read_csv(file_path)
                    
                    # Convert to records and handle NaN values
                    records = df.fillna("").to_dict(orient="records")
                    
                    # Structure similar to Excel but with a single sheet
                    csv_data = {
                        "main": records  # Single "sheet" named "main"
                    }
                    
                    kafka_message = {
                        "file_name": file_name,
                        "file_path": file_path,
                        "file_type": "csv",
                        "data": csv_data,
                        "creation_timestamp": creation_timestamp,
                        "last_modified_timestamp": last_modified_timestamp
                    }
                    producer.send(STRUCTURED_TOPIC, value=kafka_message)
                    producer.flush()
                    print(f"+ Sent CSV file data from {file_name} to {STRUCTURED_TOPIC}.")
                    continue


                if df and not df.isEmpty():
                    structured_data = df.toJSON().collect()
                    kafka_message = {
                        "file_name": file_name,
                        "file_path": file_path,
                        "data": structured_data,
                        "creation_timestamp": creation_timestamp,
                        "last_modified_timestamp": last_modified_timestamp
                    }

                    producer.send(STRUCTURED_TOPIC, value=kafka_message)
                    producer.flush()
                    print(f"+ Sent structured data from {file_name} to {STRUCTURED_TOPIC}.")

            except Exception as e:
                print(f" !!! Error processing {file_name}: {e}")

        # Process Word/PDF
        elif file_extension in ['.docx', '.pdf']:
            kafka_message = {
                "file_name": file_name,
                "file_path": file_path,
                "size": file_size,
                "creation_timestamp": creation_timestamp,
                "last_modified_timestamp": last_modified_timestamp
            }

            producer.send(UNSTRUCTURED_TOPIC, value=kafka_message)
            producer.flush()
            print(f"+ Sent unstructured data file {file_name} to {UNSTRUCTURED_TOPIC}.")

print(" +++ All files processed successfully!")
