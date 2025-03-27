import os
import streamlit as st
import sqlite3
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
from langchain.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# --- Config ---
INDEX_PATH = "/Users/devarsh/Documents/University of Washington/Coursework/Capstone/Capstone_Q5/KakfaStreamPOC/KafkaStreamPoC/faiss_index.index"
DB_PATH = "/Users/devarsh/Documents/University of Washington/Coursework/Capstone/Capstone_Q5/KakfaStreamPOC/KafkaStreamPoC/kafka_messages.db"

# --- Load embedding model ---
@st.cache_resource
def load_embedder():
    return SentenceTransformer("all-MiniLM-L6-v2")

# --- Load FAISS Index ---
@st.cache_resource
def load_faiss_index():
    return faiss.read_index(INDEX_PATH)

# --- Query FAISS ---
def search_chunks(query, embedder, index, k=1):
    query_vec = embedder.encode([query]).astype("float32")
    distances, indices = index.search(query_vec, k)
    return indices[0]

# --- Fetch chunks from DB ---
def fetch_chunks(indices):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    data = []
    for idx in indices:
        cursor.execute("SELECT chunk_text, original_doc_name, file_path FROM vector_store WHERE faiss_id = ?", (str(idx),))
        row = cursor.fetchone()
        if row:
            data.append({"text": row[0], "doc": row[1], "path": row[2]})
    conn.close()
    return data

# --- Generate Answer using Mistral via Ollama ---
def generate_answer(context_chunks, query):
    context = "\n\n".join([chunk["text"] for chunk in context_chunks])

    prompt_template = PromptTemplate(
        input_variables=["context", "question"],
        template="""
        You are a helpful assistant. Given the following document chunks and a question, generate a concise and accurate answer.

        Document Chunks:
        {context}

        Question: {question}
        Answer:"""
    )

    llm = Ollama(model="mistral")
    chain = LLMChain(llm=llm, prompt=prompt_template)
    response = chain.run({"context": context, "question": query})
    return response

# --- Streamlit UI ---
st.title("📚 Buddy AI")
st.markdown("I can help answer any question you have based on the company data and policies. Powered by FAISS + Ollama + Mistral.")

# --- Chat box for user input ---
user_query = st.chat_input("Ask a question...")


# --- Run when user submits a question ---
if user_query:
    embedder = load_embedder()
    index = load_faiss_index()

    st.session_state.chat_history.append(("User", user_query))
    with st.chat_message("user"):
        st.markdown(user_query)

    top_indices = search_chunks(user_query, embedder, index)
    chunks = fetch_chunks(top_indices)

    if chunks:
        answer = generate_answer(chunks, user_query)
        
        # Format the assistant message with sources included
        answer_with_sources = f"{answer}\n\n**📄 Source Documents:**\n"
        seen_docs = set()
        for chunk in chunks:
            doc = chunk["doc"]
            path = chunk.get("path")
            if doc not in seen_docs:
                seen_docs.add(doc)
                if path and os.path.exists(path):
                    answer_with_sources += f"- **{doc}**\n"
                else:
                    answer_with_sources += f"Source unavailable"

        # Store combined answer in chat history
        st.session_state.chat_history.append(("assistant", answer_with_sources))

        # Display assistant message
        with st.chat_message("assistant"):
            st.markdown(answer_with_sources)



for role, msg in st.session_state.chat_history:
    with st.chat_message(role.lower()):
        st.markdown(msg)


