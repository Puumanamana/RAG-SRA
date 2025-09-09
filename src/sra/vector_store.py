from pathlib import Path
from tqdm import tqdm
import json

from llama_index.core import Settings, Document, StorageContext, VectorStoreIndex
# from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.embeddings.google_genai import GoogleGenAIEmbedding
import chromadb
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.core.node_parser import SentenceSplitter


# Settings.embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")
Settings.embed_model = GoogleGenAIEmbedding(model_name="text-embedding-004", temperature=0.0)


def initialize_vector_store(outdir, collection_name="SRA_RAG"):
    """Initialize a vector store index with no documents."""
    # --- Build or load vector store index
    vector_store_path = Path(outdir, "chroma_db")
    db = chromadb.PersistentClient(path=vector_store_path)
    collection = db.get_or_create_collection(collection_name)

    # Assign chroma as the vector_store to the context
    vector_store = ChromaVectorStore(chroma_collection=collection)
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    return vector_store, storage_context


def build_new_index(documents_json, storage_context, chunk_size=1024, chunk_overlap=20):
    """Build a new vector store index from the given documents JSON file.
    For speed purposes, only include human and mouse studies.
    """
    print(f"Loading raw data from {documents_json}...")
    with open(documents_json) as f:
        data = json.load(f)

    documents = []
    for sra_id, entry in tqdm(data.items(), desc="Building documents"):
        bioproject = entry['study'].pop("bioproject", "")
        srp_id = entry['study'].pop("SRP_ID", "")

        text = "\n".join(
            [f"{k}: {v}" for k, v in entry['study'].items()] +
            [f"{k}: {v}" for k, v in entry['sample'].items()] +
            [f"{k}: {v}" for k, v in entry.get('experiment', {}).items()]
        )
        doc = Document(
            text=text,
            metadata=dict(
                sra_id=sra_id,
                species=entry['sample'].get("species", ""),
                bioproject=bioproject,
                srp_id=srp_id,
            )
        )
        documents.append(doc)

    print(f"Building vector store index on ({len(documents):,} documents)...")
    return VectorStoreIndex.from_documents(
            documents, 
            storage_context=storage_context,
            transformations=[SentenceSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)],
            show_progress=True
        )


def get_vector_store_index(outdir):
    (vector_store, storage_context) = initialize_vector_store(outdir)
    return VectorStoreIndex.from_vector_store(
        vector_store, 
        storage_context=storage_context
    )


if __name__ == "__main__":
    outdir = "./sra-rag-data-full"

    inputs = f"{outdir}/sra_data.json"
    vector_store, storage_context = initialize_vector_store(outdir)
    build_new_index(inputs, storage_context)