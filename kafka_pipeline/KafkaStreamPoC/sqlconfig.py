import sqlite3

# Connect to SQLite database
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

# To create 'structured_data' table (for JSON/XML)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS structured_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        data TEXT NOT NULL,
        creation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_modified_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        chunked BOOLEAN DEFAULT FALSE
    )
''')

# To create 'unstructured_data' table (for Word/PDF)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS unstructured_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        size INTEGER,
        creation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_modified_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        chunked BOOLEAN DEFAULT FALSE
    )
''')

# To create 'vector_store' table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS vector_store (
        chunk_id TEXT PRIMARY KEY,
        faiss_id TEXT,
        original_doc_id TEXT NOT NULL,
        original_doc_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        chunk_text TEXT NOT NULL,
        embedding_vector BLOB NOT NULL  -- Stores FAISS vector
    )
''')

conn.commit()
conn.close()

print("Database tables created successfully.")