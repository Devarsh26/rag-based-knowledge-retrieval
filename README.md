# RAG-Based Knowledge Retrieval

A scalable and multimodal pipeline based AI-powered chatbot assistant that allows enterprise users to retrieve knowledge from internal documents using a Retrieval-Augmented Generation (RAG) approach.

## Purpose

- Reduce query resolution time
- Improve access to the organization’s knowledge base
- Minimize dependency on support teams
- Uses RAG based AI to deliver direct answers from internal documents (PDF, Word, JSON, XML, Excel)

## System Architecture / Workflow

- Source files: SharePoint (PDF, Word, JSON, XML, Excel)
- Data streamed via Kafka + PySpark
- Documents stored in SQLite
- Text processed with:
    - Chunking (LangChain SemanticChunker)
    - Embedding (HuggingFace MiniLM)
    - Stored in FAISS Vector DB
Semantic similarity retrieves relevant chunks
Mistral LLM (Ollama, local) answers using [Context + Query]
Frontend built with Streamlit

## Tech Stack

- Infrastructure: Docker
- Pipeline: Kafka + PySpark
- Storage: SQLite
- Chunking: LangChain SemanticChunker
- Embeddings: HuggingFace (MiniLM)
- Vector DB: FAISS + SQLite
- LLM: Mistral via Ollama
- UI: Streamlit

## Setup Instructions / Execution

1. **Clone this repository**  
       git clone https://github.com/Devarsh26/rag-based-knowledge-retrieval.git

       cd rag-based-knowledge-retrieval/kakfa_pipeline/KafkasTREAMPoC

3. **Start Docker containers**
4. **Set up your virtual environment**
5. **Install dependencies**

       pip install --upgrade pip

       pip install -r requirements.txt

7. **Delete & recreate Kafka topics**

       Delete topics (if they exist)
        docker exec -it kafka-1-1 kafka-topics.sh --delete --topic structured-data-topic --bootstrap-server localhost:9092
        docker exec -it kafka-1-1 kafka-topics.sh --delete --topic unstructured-data-topic --bootstrap-server localhost:9092

       Recreate topics
        docker exec -it kafka-1-1 kafka-topics.sh --create --topic structured-data-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
        docker exec -it kafka-1-1 kafka-topics.sh --create --topic unstructured-data-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

9. **Recreate SQLite tables**

       rm kafka_messages.db

       python3 sqlconfig.py

11. **Run all scripts in order**

        python3 producer.py
        python3 consumer_xml:json.py
        python3 consumer_word:pdf.py
        python3 retrieve_file.py (optional: to check if files are sent correctly from producer to consumer)
        python3 data_chunking.py
        python3 read_chunks.py (optional: to check the chunks)
        python3 streamlit run application.py

## Requirements

- Python 3.8+
- Docker
- Ollama (for local Mistral model)
- Dependencies in requirements.txt

