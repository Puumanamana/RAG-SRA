from pydantic import BaseModel, Field
from typing import List

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
    tissues: List[str] = Field(description="Tissue type studied, if applicable")
    diseases: List[str] = Field(description="Disease studied, if applicable")
    sample_count: int = Field(description="Number of samples in the study")
    explanation: str = Field(description="Explanation of why this study was retrieved")

    def __str__(self):
        return (
            f"> {self.bioproject:<14}| N={self.sample_count:,}\n"
            f"Tissue(s): {', '.join(self.tissues)}\n"
            f"Disease(s): {', '.join(self.diseases)}\n"
            f"Title: {self.title}\n"
            f"Reason: {self.explanation}"
        )

class StudyList(BaseModel):
    studies: list[Study] = Field(description="List of studies")

    def __str__(self):
        msg = [str(s) for s in self.studies]
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

    query_engine = build_query_engine(outdir="./sra-rag-data-full")
    question = "Can you list Lupus studies that include skin samples?"

    # -- Test query
    text = query_engine.query(question)
    print(text.response)