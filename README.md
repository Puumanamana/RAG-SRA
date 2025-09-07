# RAG application for SRA data query

## Summary

Current study counts in SRA_Accessions.tab, as of Sep 2 2025: 713,567
- SRP IDs: 615,730
- ERP IDs: 77,351
- DRP IDs: 20,486

SRA_Accessions.tab: (type='RUN'):
- 1,824,558 accession total
- 1,023,045 SRP accessions
NCBI_SRA_Metadata_Full_20250818: 7,036,375 studies, 566,321 study files 
NCBI_SRA_Metadata_20250901: 10,596 studies, ~2k study files

## Data

| File            | Top level      |    Accession   |      Parent ID      | Descr                                                                |
|-----------------|----------------|----------------|---------------------|----------------------------------------------------------------------|
| study.xml       | STUDY_SET      | SRP/BioProject |                     | Defines the research project: scope, title, abstract, external links |
| sample.xml      | SAMPLE_SET     | SRS/BioSample  |      BioProject     | Defines the biological samples collected for the study               |
| experiment.xml  | EXPERIMENT_SET |     SRX        | SRS/SRP/BioProject  | Defines how a given sample was processed/prepared for sequencing     |
| run.xml         | RUN_SET        |     SRR        |    EXPERIMENT       | Defines the actual sequencing data runs produced                     |

**Fields to keep**

- STUDY_SET: STUDY/DESCRIPTOR
- SAMPLE_SET: TITLE, SAMPLE_NAME, DESCRIPTION, SAMPLE_ATTRIBUTES
- EXPERIMENT_SET: TITLE, DESIGN/DESIGN_DESCR, DESIGN//LIBRARY_DESCRIPTOR/{LIBRARY_NAME,LIBRARY_STRATEGY,LIBRARY_SOURCE,LIBRARY_SELECTION,LIBRARY_LAYOUT}, PLATFORM/TECHNOLOGY_FLAG/INSTRUMENT_MODEL
- RUN_SET: RUN_ATTRIBUTES

## Approach

(1) Data extraction
- We collect latest study metadata by downloading the latest SRA metadata dump: https://ftp-trace.ncbi.nlm.nih.gov/sra/reports/Metadata/NCBI_SRA_Metadata_DATE.tar.gz
- We preprocess each file and collect relevant fields
- We convert to JSON
- We store in different folders study, sample, experiment and run info (might want to also try combining the last 3)

(2) Feature extraction & vector database (Llama Index)

(3) RAG system
- Which LLM model?

(4) Chatbot app (LangChain)