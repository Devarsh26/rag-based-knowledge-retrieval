from kafka import KafkaConsumer
import sqlite3
import json

# Kafka Consumer Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "structured-data-topic"

# Create Kafka consumer
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
    structured_data = json.dumps(data["data"])
    creation_timestamp = data.get("creation_timestamp", None)
    last_modified_timestamp = data.get("last_modified_timestamp", None)

    # Check if the file exists and compare timestamps
    cursor.execute("SELECT last_modified_timestamp FROM structured_data WHERE file_name = ? ORDER BY last_modified_timestamp DESC LIMIT 1", (file_name,))
    result = cursor.fetchone()

    if result:
        existing_timestamp = result[0]
        if existing_timestamp == last_modified_timestamp:
            print(f"⏩ Skipping unchanged file: {file_name}")
            continue  # Skip file if unchanged
        else:
            # Delete old chunks for that file
            cursor.execute("DELETE FROM vector_store WHERE original_doc_name = ?", (file_name,))
            print(f"🗑️ Deleted old chunks for {file_name} from vector_store.")

            # Delete the old metadata
            cursor.execute("DELETE FROM structured_data WHERE file_name = ?", (file_name,))
            print(f"🗑️ Deleted old version of {file_name} from structured_data.")


    # Insert new file or new version if modified
    cursor.execute('''
        INSERT INTO structured_data (file_name, file_path, data, creation_timestamp, last_modified_timestamp)
        VALUES (?, ?, ?, ?, ?)
    ''', (file_name, file_path, structured_data, creation_timestamp, last_modified_timestamp))

    conn.commit()
    print(f"✅ Stored structured data for {file_name} in database.")

conn.close()
