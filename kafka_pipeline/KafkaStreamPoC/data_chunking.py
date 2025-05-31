import os
import json
import pdfplumber
import textract
import uuid
import faiss
import numpy as np
import sqlite3
from tqdm import tqdm
from langchain_experimental.text_splitter import SemanticChunker
from sentence_transformers import SentenceTransformer

# Initialized FAISS Index
vector_dimension = 384  # Hugging Face embedding dimension (all-MiniLM-L6-v2)
flat_index = faiss.IndexFlatL2(vector_dimension)
index = faiss.IndexIDMap(flat_index)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # current script folder
INDEX_PATH = os.path.join(BASE_DIR, "faiss_index.index")

# Loadeds HuggingFace Sentence Transformer model
model = SentenceTransformer('all-MiniLM-L6-v2')


class HFEmbeddingWrapper:
    """Wrapper to make Hugging Face embeddings compatible with LangChain's SemanticChunker."""
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)

    def embed_documents(self, texts):
        """Converts a list of texts into embeddings using Hugging Face's SentenceTransformer."""
        return self.model.encode(texts)

# Load Hugging Face model with compatibility wrapper
hf_embeddings = HFEmbeddingWrapper()



# Function to fetch structured data from SQLite
def fetch_structured_data():
    conn = sqlite3.connect("kafka_messages.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, file_name, data, file_path FROM structured_data where chunked = FALSE")
    structured_data = cursor.fetchall()
    conn.close()
    return structured_data

# Function to fetch unstructured data from SQLite
def fetch_unstructured_data():
    conn = sqlite3.connect("kafka_messages.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id, file_name, file_path FROM unstructured_data where chunked = FALSE")
    unstructured_data = cursor.fetchall()
    conn.close()
    return unstructured_data

def extract_text_from_file(file_path):
    """Extracts text from PDFs and Word files while handling encoding issues."""
    try:
        if file_path.endswith(".pdf"):
            text = ""
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
            return text.strip() if text else "PDF extraction failed!"
        
        elif file_path.endswith(".docx") or file_path.endswith(".doc"):
            text = textract.process(file_path).decode("utf-8", errors="ignore")
            return text.strip()

        else:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as file:
                return file.read().strip()

    except Exception as e:
        print(f"Error extracting text from {file_path}: {e}")
        return "Error extracting text!"

# Function to perform semantic chunking
def semantic_chunk_text(text):
    splitter = SemanticChunker(hf_embeddings, breakpoint_threshold_type="percentile")
    return splitter.create_documents([text])

def generate_faiss_id(uuid_str):
    return uuid.UUID(uuid_str).int & (1<<63)-1

# Function to process chunks and store in SQLite database
def process_chunks(chunks, original_doc_id, original_doc_name, file_path):
    conn = sqlite3.connect("kafka_messages.db")
    cursor = conn.cursor()

    for chunk in tqdm(chunks, desc="Embedding & Storing Chunks"):
        chunk_id = str(uuid.uuid4())
        chunk_text = chunk.page_content if hasattr(chunk, "page_content") else str(chunk)

        vector = model.encode(chunk_text)
        vector_bytes = np.array(vector, dtype=np.float32).tobytes()

        faiss_id = generate_faiss_id(chunk_id)

        # Store FAISS vector in SQLite
        cursor.execute("INSERT INTO vector_store (chunk_id, faiss_id, original_doc_id, original_doc_name, file_path, chunk_text, embedding_vector) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (chunk_id, str(faiss_id), original_doc_id, original_doc_name, file_path, chunk.page_content, vector.tobytes()))
        conn.commit()

        # Add vector to FAISS index
        index.add_with_ids(np.array([vector], dtype=np.float32), np.array([faiss_id], dtype=np.int64))

    faiss.write_index(index, INDEX_PATH)

    conn.close()

# Function to process structured data
def process_structured_data():
    structured_files = fetch_structured_data()
    
    for id, file_name, data, file_path in tqdm(structured_files, desc="Processing Structured Data"):
        parsed_data = json.loads(data)

        combined_text = ""

        if isinstance(parsed_data, dict):
            # Excel case: dict of sheets
            if all(isinstance(v, list) for v in parsed_data.values()):
                for sheet_name, rows in parsed_data.items():
                    sheet_text = ""
                    for row in rows:
                        if isinstance(row, dict):
                            clean_row = {k.strip(): v for k, v in row.items() if k and "Unnamed" not in k and v != ""}
                            if clean_row:
                                sheet_text += json.dumps(clean_row) + "\n"
                    if sheet_text:
                        chunks = semantic_chunk_text(sheet_text)
                        process_chunks(chunks, id, f"{file_name} - {sheet_name}", file_path)
                continue
            else:
                combined_text = " ".join(str(value) for value in parsed_data.values())

        elif isinstance(parsed_data, list):
            if all(isinstance(item, dict) for item in parsed_data):
                combined_text = "\n".join(json.dumps(item) for item in parsed_data)
            elif all(isinstance(item, str) and item.strip().startswith("{") for item in parsed_data):
                combined_text = "\n".join(parsed_data)
            else:
                combined_text = " ".join(str(item) for item in parsed_data)

        else:
            combined_text = str(parsed_data)

        if combined_text:
            chunks = semantic_chunk_text(combined_text)
            process_chunks(chunks, id, file_name, file_path)

        conn = sqlite3.connect("kafka_messages.db")
        cursor = conn.cursor()
        cursor.execute("UPDATE structured_data SET chunked = TRUE WHERE file_name = ?", (file_name,))
        conn.commit()



# Function to process unstructured data
def process_unstructured_data():
    unstructured_files = fetch_unstructured_data()

    for id, file_name, file_path in tqdm(unstructured_files, desc="Processing Unstructured Data"):
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} not found. Skipping...")
            continue

        text = extract_text_from_file(file_path)

        if text == "Error extracting text!":
            print(f"Skipping {file_name} due to extraction failure.")
            continue

        chunks = semantic_chunk_text(text)
        process_chunks(chunks, id, file_name, file_path)

        conn = sqlite3.connect("kafka_messages.db")
        cursor = conn.cursor()
        cursor.execute("UPDATE unstructured_data SET chunked = TRUE WHERE file_name = ?", (file_name,))
        conn.commit()


# Process both structured and unstructured data
process_structured_data()
process_unstructured_data()
