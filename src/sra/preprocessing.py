import ftplib
from collections import defaultdict
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from enum import Enum
import json


class SRAFileType(Enum):
    SUBMISSION = 'submission'
    STUDY = 'study'
    SAMPLE = 'sample'
    EXPERIMENT = 'experiment'
    RUN = 'run'
    ANALYSIS = 'analysis'


class SRAFileParser:
    """Class for 
    (1) Downloading SRA studies dump from FTP server
    (2) Parsing resulting XML files and extracting relevant fields
    (3) Saving to JSON
    """
    def __init__(self, outdir="./sra-rag-data"):
        self.ftp_root = "ftp-trace.ncbi.nlm.nih.gov"
        self.ftp_path = "/sra/reports/Metadata"
        self.outdir = Path(outdir)
        self.local_dump = None

    def download_sra_from_ftp(self, dump_date):
        """Downloads SRA dump from the FTP server for the specified date
        :param dump_date: File dump extension in SRA FTP (format: YYYYMMDD)
            Tested with '20250901'
        """
        filename = f"NCBI_SRA_Metadata_{dump_date}.tar.gz"
        local_filepath = Path(self.outdir, filename)

        if not self.outdir.exists():
            self.outdir.mkdir()

        if not local_filepath.exists():
            ftp = ftplib.FTP(self.ftp_root)
            ftp.login()

            print(f"Downloading SRA dump")
            try:
                ftp.retrbinary(
                    f"RETR {self.ftp_path}/{filename}",
                    open(local_filepath, "wb").write
                )
            except ftplib.error_perm as e:
                raise ValueError(
                    f"File dump not found, check {self.ftp_root}/{self.ftp_path.lstrip('/')} "
                    f"to make sure the selected dump_date is correct ({dump_date})"
                )

        self.local_dump = local_filepath

    def iter_sra(self):
        """Iterates over the SRA XML files in the local dump
        """
        assert self.local_dump is not None, "SRA local dump not found, run download_sra_from_ftp() first"
        with tarfile.open(fileobj=open(self.local_dump, "rb"), mode="r:gz") as tar:
            for member in tar:
                if member.name.endswith(".xml"):
                    file_member = tar.extractfile(member)
                    yield file_member

    def parse_db(self):
        """Parse SRA XML files and extract relevant fields
        Write extracted data for each file type in a JSON file
        """
        data = {}
        last_id = None
        for i, fh in enumerate(self.iter_sra(), 1):
            if i % 1000 == 0:
                print(f"Processed {i:,} entries ({len(data):,} kept)")

            sra_id = Path(fh.name).parent.name
            suffix = Path(fh.name).suffixes[0].lstrip('.').upper()

            try:
                file_type = SRAFileType[suffix]
            except KeyError:
                print(f"Unknown file suffix for: {fh.name}")
                continue

            if file_type == SRAFileType.STUDY:
                entry = self.parse_study_xml(fh)
            elif file_type == SRAFileType.SAMPLE:
                entry = self.parse_sample_xml(fh)
                key = list(entry.keys())[0]
                n_samples = sum(entry[key].values())
                if n_samples == 1:
                    # Skip datasets with one sample
                    continue
            elif file_type == SRAFileType.EXPERIMENT:
                entry = self.parse_experiment_xml(fh)
            elif file_type in {SRAFileType.RUN, SRAFileType.SUBMISSION, SRAFileType.ANALYSIS}:
                continue

            summary = self.aggregate_results(entry)
            data.setdefault(sra_id, {}).update({file_type.name.lower(): summary})

            if last_id != sra_id:
                # filter out data for memory efficiency
                if last_id is not None and last_id in data:
                    if "study" not in data[last_id] and "sample" not in data[last_id]:
                        del data[last_id]
                last_id = sra_id

        # Clean up data: remove entries without study or sample info
        data = {k: v for k, v in data.items() if "study" in v and "sample" in v}

        # Write each file type to its own JSON file
        print(f"Writing to JSON, total studies: {len(data):,}")
        with open(Path(self.outdir, f"sra_data.json"), "w") as out_fh:
            json.dump(data, out_fh, indent=2)

    def parse_study_xml(self, file_obj):
        """Parse study.xml file for a given study
        Keep following fields:
        - IDENTIFIERS/{PRIMARY_ID, EXTERNAL_IDs}
        - DESCRIPTOR/{STUDY_TITLE, STUDY_ABSTRACT, STUDY_TYPE['existing_study_type']}
        """
        data = {k: defaultdict(int) for k in ["SRP_ID", "bioproject", "title", "study_type", "abstract"]}

        for _, elem in ET.iterparse(file_obj, events=("end",)):
            if elem.tag == "STUDY":
                # String fields
                SRP_ID = elem.findtext("IDENTIFIERS/PRIMARY_ID")
                bioproject = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='BioProject']")
                # GSE_ID = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='GEO']")
                title = elem.findtext("DESCRIPTOR/STUDY_TITLE")
                abstract = elem.findtext("DESCRIPTOR/STUDY_ABSTRACT")
                
                # Dict field
                study_type_elem = elem.find("DESCRIPTOR/STUDY_TYPE")
                study_type = study_type_elem.get("existing_study_type", "") if study_type_elem is not None else ""

                # Add to JSON data
                data["SRP_ID"][SRP_ID] += 1
                data["bioproject"][bioproject] += 1
                data["title"][title] += 1
                data["study_type"][study_type] += 1
                data["abstract"][abstract] += 1

                elem.clear()

        return data

    def parse_sample_xml(self, file_obj):
        """Parse sample.xml file for a given study
        Keep following fields:
        - IDENTIFIERS/{PRIMARY_ID, EXTERNAL_IDs}
        - TITLE
        - SAMPLE_NAME/SCIENTIFIC_NAME
        - SAMPLE_ATTRIBUTES[List[Tuple[str, str]]]
        """
        data = {"title": defaultdict(int), "species": defaultdict(int)}

        for _, elem in ET.iterparse(file_obj, events=("end",)):
            if elem.tag == "SAMPLE":
                # String fields
                # SRS_ID = elem.findtext("IDENTIFIERS/PRIMARY_ID")
                # GSM_ID = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='GEO']")
                # biosample = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='BioSample']")
                title = elem.findtext("TITLE")
                species = elem.findtext("SAMPLE_NAME/SCIENTIFIC_NAME")

                # List field
                sample_attributes_elem = elem.find("SAMPLE_ATTRIBUTES")  # Iterable of Tuples
                attributes = {tag.text: ' '.join(str(v.text) for v in value) for (tag, *value) in sample_attributes_elem} if sample_attributes_elem is not None else {}

                if title is not None:
                    data["title"][title] += 1
                if species is not None:
                    data["species"][species] += 1
                for k, v in attributes.items():
                    data.setdefault(k, defaultdict(int))[v] += 1
                elem.clear()

        return data

    def parse_experiment_xml(self, file_obj):
        """Parse experiment.xml file for a given study
        Keep following fields:
        - IDENTIFIERS/PRIMARY_ID
        - STUDY_REF/IDENTIFIERS/{PRIMARY_ID,EXTERNAL_ID}
        - DESIGN/SAMPLE_DESCRIPTOR/PRIMARY_ID
        - TITLE
        - DESIGN/DESIGN_DESCRIPTION
        - DESIGN/LIBRARY_DESCRIPTOR/{LIBRARY_NAME,LIBRARY_STRATEGY,LIBRARY_SOURCE,LIBRARY_SELECTION,LIBRARY_LAYOUT}
        - PLATFORM/TECHNOLOGY_FLAG/INSTRUMENT_MODEL
        """
        data = {k: defaultdict(int) for k in [
            "title", "design_description", "library_name", "library_strategy", "library_source",
            "library_selection", "library_layout", "platform_technology", "platform_instrument"
        ]}

        for _, elem in ET.iterparse(file_obj, events=("end",)):
            if elem.tag == "EXPERIMENT":
                # String fields
                # SRX_ID = elem.findtext("IDENTIFIERS/PRIMARY_ID")
                # SRS_ID = elem.findtext("DESIGN/SAMPLE_DESCRIPTOR/PRIMARY_ID")
                # SRP_ID = elem.findtext("STUDY_REF/IDENTIFIERS/PRIMARY_ID")
                # bioproject = elem.findtext("STUDY_REF/IDENTIFIERS/EXTERNAL_ID[@namespace='BioProject']")
                title = elem.findtext("TITLE")
                design_descr = elem.findtext("DESIGN/DESIGN_DESCRIPTION")
                library = {
                    key:  elem.findtext(f"DESIGN/LIBRARY_DESCRIPTOR/{key}") for key in [
                        "LIBRARY_NAME",
                        "LIBRARY_STRATEGY",
                        "LIBRARY_SOURCE",
                        "LIBRARY_SELECTION",
                    ]
                }
                # List field (Iterable of singleton tags)
                library["LIBRARY_LAYOUT"] = "|".join(l.tag for l in elem.find("DESIGN/LIBRARY_DESCRIPTOR/LIBRARY_LAYOUT"))

                # List field with tag extraction
                platform_elem = elem.find("PLATFORM")
                platform = None
                if platform_elem is not None:
                    if len(platform_elem) > 1:
                        raise ValueError(f"Invalid PLATFORM element: {platform_elem}")

                    platform = {
                        "TECHNOLOGY": platform_elem[0].tag,
                        "INSTRUMENT_MODEL": platform_elem[0].findtext("INSTRUMENT_MODEL")
                    }

                # Add to JSON data
                if title is not None:
                    data["title"][title] += 1
                if design_descr is not None:
                    data["design_description"][design_descr] += 1
                for k, v in library.items():
                    if v is not None:
                        data[k.lower()][v] += 1
                if platform is not None:
                    data["platform_technology"][platform["TECHNOLOGY"]] += 1
                    data["platform_instrument"][platform["INSTRUMENT_MODEL"]] += 1
                elem.clear()

        return data

    def aggregate_results(self, data, min_samples=10, min_count=3):
        """Aggregate parsed results across all studies
        """
        summary = {}
        # Skip fields with too many unique values, unless there are very few samples
        for attr, count_dict in data.items():
            if not count_dict:
                continue
            if attr in {"SRP_ID", "bioproject", "title"}:
                summary[attr] = "|".join(map(str, count_dict.keys()))
            elif max(count_dict.values()) < min_count and len(count_dict) > min_samples:
                continue
            else:
                summary[attr] = "|".join(f"{value}(N={count})" for value, count in count_dict.items())
        return summary


if __name__ == "__main__":
    # parser = SRAFileParser(outdir="./sra-rag-data")
    # parser.download_sra_from_ftp(dump_date="20250901")
    parser = SRAFileParser(outdir="./sra-rag-data-full")
    parser.download_sra_from_ftp(dump_date="Full_20250818")
    data = parser.parse_db()