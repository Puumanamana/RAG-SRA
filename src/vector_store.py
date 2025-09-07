from pathlib import Path
from tqdm import tqdm
import json

from llama_index.core import Settings, Document, StorageContext, VectorStoreIndex
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
# from llama_index.embeddings.google_genai import GoogleGenAIEmbedding
import chromadb
from llama_index.vector_stores.chroma import ChromaVectorStore


Settings.embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")


def initialize_vector_store(outdir="./rag_data"):
    """Initialize a vector store index with no documents."""
    # --- Build or load vector store index
    vector_store_path = Path(outdir, "chroma_db")
    db = chromadb.PersistentClient(path=vector_store_path)
    collection = db.get_or_create_collection("SRA-RAG")

    # Assign chroma as the vector_store to the context
    vector_store = ChromaVectorStore(chroma_collection=collection)
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    return vector_store, storage_context


def build_new_index(documents_json, storage_context):
    """Build a new vector store index from the given documents JSON file.
    For speed purposes, only include human and mouse studies.
    """
    with open(documents_json) as f:
        studies = json.load(f)

    documents = []
    for study in tqdm(studies, desc="Building documents"):
        org = study.get("organism", "")
        acc = study.get("accession", "")

        if org in {"Homo sapiens", "Mus musculus"}:
            text = study.get("description", "")
            # pubs = "\n".join([f"{p.get('title','')} ({p.get('journal','')}, {p.get('year','')})" for p in study.get("publications", []) if p.get('title')])
            doc = Document(
                text=f"Organism: {org}\nDescription: {text}", #\nPublications: {pubs}",
                metadata={
                    "bioproject": acc,
                }
            )
            documents.append(doc)

            if len(documents) > 100:
                break
    
    print(f"Building vector store index on ({len(documents):,} documents)...")
    return VectorStoreIndex.from_documents(
            documents, 
            storage_context=storage_context,
            show_progress=True
        )


def get_vector_store_index(outdir):
    (vector_store, storage_context) = initialize_vector_store(outdir)
    return VectorStoreIndex.from_vector_store(
        vector_store, 
        storage_context=storage_context
    )


if __name__ == "__main__":
    input = "./rag-data/bioprojects.json"
    outdir = "./rag-data"

    if Path(outdir, "chroma_db").exists():
        print("Skipping, vector store already exists.")
    else:
        vector_store, storage_context = initialize_vector_store(outdir)
        build_new_index(input, storage_context)