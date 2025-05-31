from kafka import KafkaConsumer
import sqlite3
import json

# Kafka consumer setup
KAFKA_BROKER = "localhost:9092"
TOPIC = "unstructured-data-topic"

# Created Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    consumer_timeout_ms=10000
)

# Connect to SQLite database
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

print(f"Listening for messages on {TOPIC}...")

# Consume messages from Kafka
for message in consumer:
    data = message.value
    file_name = data["file_name"]
    file_path = data["file_path"]
    file_size = data["size"]
    creation_timestamp = data["creation_timestamp"]
    last_modified_timestamp = data["last_modified_timestamp"]

    # Check if the file exists and compare timestamps
    cursor.execute("SELECT last_modified_timestamp FROM unstructured_data WHERE file_name = ? ORDER BY last_modified_timestamp DESC LIMIT 1", (file_name,))
    result = cursor.fetchone()

    if result:
        existing_timestamp = result[0]
        if existing_timestamp == last_modified_timestamp:
            print(f"/ Skipping unchanged file: {file_name}.")
            continue  # Skip file if unchanged
        else:
            # Delete old chunks for that file
            cursor.execute("DELETE FROM vector_store WHERE original_doc_name = ?", (file_name,))
            print(f"- Deleted old chunks for {file_name} from vector_store table.")

            # Delete the old metadata
            cursor.execute("DELETE FROM unstructured_data WHERE file_name = ?", (file_name,))
            print(f"- Deleted old version of {file_name} from unstructured_data table.")

    # Insert new file or new version if modified
    cursor.execute('''
        INSERT INTO unstructured_data (file_name, file_path, size, creation_timestamp, last_modified_timestamp)
        VALUES (?, ?, ?, ?, ?)
    ''', (file_name, file_path, file_size, creation_timestamp, last_modified_timestamp))

    conn.commit()
    print(f"+ Stored unstructured file's file path & metadata for {file_name} in database.")

conn.close()
