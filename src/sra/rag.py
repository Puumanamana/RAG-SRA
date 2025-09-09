from pydantic import BaseModel, Field

from llama_index.core import get_response_synthesizer
from llama_index.core.query_engine import RetrieverQueryEngine
from llama_index.core.postprocessor import SimilarityPostprocessor
from llama_index.core.retrievers import VectorIndexRetriever
from llama_index.llms.google_genai import GoogleGenAI

from vector_store import get_vector_store_index


# -- Pydantic models for structured response parsing
class Study(BaseModel):
    bioproject: str = Field(description="BioProject accession")
    title: str = Field(description="Shortened title of the study")
    explanation: str = Field(description="Explanation of why this study was retrieved")

class StudyList(BaseModel):
    studies: list[Study] = Field(description="List of studies")

    def __str__(self):
        msg = [f"> {s.bioproject:<14}\nReason: {s.explanation}\nTitle: {s.title}" for s in self.studies]
        return f"#Hits: {len(msg)}\n" + "\n".join(msg)
    

def build_query_engine(k=30, similiarity_cutoff=0.4, outdir="./bioprojects-rag-data", model="models/gemini-2.0-flash-lite"):
    # -- Retriever
    index = get_vector_store_index(outdir)
    retriever = VectorIndexRetriever(index=index, similarity_top_k=k)

    # -- LLM
    llm = GoogleGenAI(model=model).as_structured_llm(StudyList)

    # -- Configure response synthesizer
    response_synthesizer = get_response_synthesizer(llm=llm)

    # -- Assemble query engine
    return RetrieverQueryEngine(
        retriever=retriever,
        response_synthesizer=response_synthesizer,
        node_postprocessors=[SimilarityPostprocessor(similarity_cutoff=similiarity_cutoff)],
    )


if __name__ == "__main__":

    query_engine = build_query_engine(outdir="./sra-rag-data")
    question = "What are the Lupus studies?"

    # -- Test query
    text = query_engine.query(question)
    print(text.response)
