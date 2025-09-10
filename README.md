# RAG application for SRA data query

## Data summary

NCBI_SRA_Metadata_20250901: 10,596 studies
NCBI_SRA_Metadata_Full_20250818: 7,036,375 studies

For efficiency, we filter those studies:
- Discard studies with only 1 sample
- Discard studies with missing study or sample info
- Discard non human or mouse studies

After filtering, we have:
NCBI_SRA_Metadata_20250901: 548 studies
NCBI_SRA_Metadata_Full_20250818: 138,758 studies

## Data

| File            | Top level      |    Accession   |      Parent ID      | Descr                                                                |
|-----------------|----------------|----------------|---------------------|----------------------------------------------------------------------|
| study.xml       | STUDY_SET      | SRP/BioProject |                     | Defines the research project: scope, title, abstract, external links |
| sample.xml      | SAMPLE_SET     | SRS/BioSample  |      BioProject     | Defines the biological samples collected for the study               |
| experiment.xml  | EXPERIMENT_SET |     SRX        | SRS/SRP/BioProject  | Defines how a given sample was processed/prepared for sequencing     |
| run.xml         | RUN_SET        |     SRR        |    EXPERIMENT       | Defines the actual sequencing data runs produced                     |
| analysis.xml    | ignored        |     ignored    |    ignored          | Ignored                                                              |

**Useful fields for data extraction**

- STUDY_SET: STUDY/DESCRIPTOR
- SAMPLE_SET: TITLE, SAMPLE_NAME, DESCRIPTION, SAMPLE_ATTRIBUTES
- EXPERIMENT_SET: TITLE, DESIGN/DESIGN_DESCR, DESIGN/LIBRARY_DESCRIPTOR/{LIBRARY_NAME,LIBRARY_STRATEGY,LIBRARY_SOURCE,LIBRARY_SELECTION,LIBRARY_LAYOUT}, PLATFORM/TECHNOLOGY_FLAG/INSTRUMENT_MODEL
- RUN_SET: RUN_ATTRIBUTES

## Approach

(1) Data extraction
- We collect latest study metadata by downloading the latest SRA metadata dump: https://ftp-trace.ncbi.nlm.nih.gov/sra/reports/Metadata/NCBI_SRA_Metadata_DATE.tar.gz
- We preprocess each file and collect relevant fields
- We "deduplicate" sample/experiment level data, and only show unique values with their frequencies
- We write in a JSON file all of the study metadata (focus on study, sample & experiment, little info in run.xml)

(2) Feature extraction & vector database (Llama Index)
- Embedding: GoogleGenAIEmbedding(), or HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5") if we hit rate limits
- Vector database: ChromaDB for convenience (+open source)

(3) RAG system
- GoogleGenAI("models/gemini-2.0-flash-lite"), for simplicity
