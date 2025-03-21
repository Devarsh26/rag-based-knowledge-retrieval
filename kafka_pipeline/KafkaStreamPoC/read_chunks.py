import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

# Fetch all chunks from the vector store
cursor.execute("SELECT chunk_id, original_doc_id, original_doc_name, chunk_text FROM vector_store")
chunks = cursor.fetchall()

# Close the connection
conn.close()

# Display the retrieved chunks
if not chunks:
    print("⚠️ No chunks found in the database!")
else:
    print(f"✅ Retrieved {len(chunks)} chunks from SQLite:")
    print("="*50)
    for chunk_id, original_doc_id, original_doc_name, chunk_text in chunks:
        print(f"📄 Chunk ID: {chunk_id}")
        print(f"📄 Document ID: {original_doc_id}")
        print(f"📄 Document Name: {original_doc_name}")
        print(f"📝 Chunk Text:\n{chunk_text}")
        print("-"*50)
