import ftplib
from collections import Counter
import tarfile
from pathlib import Path
from enum import Enum
import io
import json
import xml.etree.ElementTree as ET


class SRAFileType(Enum):
    """Enum for different SRA file types in tarball
    """
    SUBMISSION = "submission"
    STUDY = "study"
    SAMPLE = "sample"
    EXPERIMENT = "experiment"
    RUN = "run"
    ANALYSIS = "analysis"

    @classmethod
    def extract(cls, filename):
        suffix = Path(filename).stem.split('.')[-1].upper()
        try:
            return cls[suffix]
        except KeyError:
            print(f"Unknown file suffix for: {filename}")
            return None


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
        Yield entries folder by folder (1 folder = 1 study = multiple XML files)
        """
        assert self.local_dump is not None, "SRA local dump not found, run download_sra_from_ftp() first"

        files = {}
        last_sra_id = None
        with tarfile.open(fileobj=open(self.local_dump, "rb"), mode="r:gz") as tar:
            for member in tar:
                path = Path(member.name)

                if member.isdir():  # Yield previous study data
                    if files:
                        yield (last_sra_id, files)
                    last_sra_id = path.name
                    files = {}
                    continue

                elif member.name.endswith(".xml"):
                    assert path.parent.name == last_sra_id, f"Unexpected folder structure: {member.name}"
                    file_type = SRAFileType.extract(member.name)
                    
                    if file_type is not None:
                        file_member = tar.extractfile(member)
                        files[file_type.name.lower()] = io.BytesIO(file_member.read())
            if files:
                yield (last_sra_id, files)

    def iterparse_sra(self):
        """Parse SRA XML files and extract relevant fields
        Write extracted data for each file type in a JSON file
        """
        for i, (sra_id, files) in enumerate(self.iter_sra(), 1):
            if i % 1000 == 0:
                print(f"Processed {i:,} studies", end='\r')

            # Skip incomplete entries
            if "study" not in files or "sample" not in files:
                continue
            
            study_info = self.parse_study_xml(files["study"])
            sample_info = self.parse_sample_xml(files["sample"])
            experiment_info = self.parse_experiment_xml(files["experiment"]) if "experiment" in files else None

            species = sample_info["species"]

            # Skip studies with <= 1 sample
            if sum(species.values()) <= 1:
                continue
            # Only keep human and mouse studies
            if not any(s in species for s in ["Homo sapiens", "Mus musculus"]):
                continue

            yield self.format_study_data(
                sra_id, 
                self.aggregate_results(study_info), 
                self.aggregate_results(sample_info), 
                self.aggregate_results(experiment_info)
            )

    @staticmethod
    def parse_study_xml(file_obj):
        """Parse study.xml file for a given study
        Keep following fields:
        - IDENTIFIERS/{PRIMARY_ID, EXTERNAL_IDs}
        - DESCRIPTOR/{STUDY_TITLE, STUDY_ABSTRACT, STUDY_TYPE['existing_study_type']}
        """
        data = {k: Counter() for k in ["SRP_ID", "bioproject", "title", "study_type", "abstract"]}

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

    @staticmethod
    def parse_sample_xml(file_obj):
        """Parse sample.xml file for a given study
        Keep following fields:
        - IDENTIFIERS/{PRIMARY_ID, EXTERNAL_IDs}
        - TITLE
        - SAMPLE_NAME/SCIENTIFIC_NAME
        - SAMPLE_ATTRIBUTES[List[Tuple[str, str]]]
        """
        data = {"title": Counter(), "species": Counter()}

        for _, elem in ET.iterparse(file_obj, events=("end",)):
            if elem.tag == "SAMPLE":
                # String fields
                # SRS_ID = elem.findtext("IDENTIFIERS/PRIMARY_ID")
                # GSM_ID = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='GEO']")
                # biosample = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='BioSample']")
                title = elem.findtext("TITLE") 
                species = elem.findtext("SAMPLE_NAME/SCIENTIFIC_NAME") or "NA"

                # List field
                sample_attributes_elem = elem.find("SAMPLE_ATTRIBUTES")  # Iterable of Tuples
                attributes = {tag.text: ' '.join(v.text for v in value if v.text) for (tag, *value) in sample_attributes_elem} if sample_attributes_elem is not None else {}

                data["species"][species] += 1
                if title is not None:
                    data["title"][title] += 1
                for k, v in attributes.items():
                    data.setdefault(k, Counter())[v] += 1
                elem.clear()

        return data

    @staticmethod
    def parse_experiment_xml(file_obj):
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
        data = {k: Counter() for k in [
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

    @staticmethod
    def aggregate_results(data, min_samples=10, min_count=3):
        """Aggregate parsed results across all studies
        """
        if data is None:
            return {}
        summary = {}
        # Skip fields with too many unique values, unless there are very few samples
        for attr, count_dict in data.items():
            if not count_dict:
                continue
            elif attr in {"SRP_ID", "bioproject", "title"}:
                summary[attr] = "|".join(map(str, count_dict.keys()))
            elif max(count_dict.values()) < min_count and len(count_dict) > min_samples:
                continue
            else:
                summary[attr] = "|".join(f"{value}(N={count})" for value, count in count_dict.items())
        return summary

    @staticmethod
    def format_study_data(sra_id, study_info, sample_info, experiment_info=None):
        """Format study data into a single text block and metadata dictionary
        """
        bioproject = study_info.pop("bioproject", "")
        srp_id = study_info.pop("SRP_ID", "")
        species = sample_info.get("species", "").lower()
        text = "\n".join(
            [f"{k}: {v}" for k, v in study_info.items()] +
            [f"{k}: {v}" for k, v in sample_info.items()] +
            ([f"{k}: {v}" for k, v in experiment_info.items()] if experiment_info else [])
        )
        metadata = dict(
            sra_id=sra_id,
            bioproject=bioproject[:300],
            srp_id=srp_id[:300],
            species=species[:300],
        )
        return dict(text=text, metadata=metadata)


if __name__ == "__main__":
    # Option 1: Smaller subset (for testing)
    # parser = SRAFileParser(outdir="./sra-rag-data")
    # parser.download_sra_from_ftp(dump_date="20250901")

    # Option 2: Full set of studies
    parser = SRAFileParser(outdir="./sra-rag-data-full")
    parser.download_sra_from_ftp(dump_date="Full_20250818")

    # Loop over studies and write to JSON
    count = 0
    with open(f"{parser.outdir}/sra-data.json", "w", encoding="utf-8") as f:
        f.write("[\n")
        first = True
        for entry in parser.iterparse_sra():
            if not first:
                f.write(",\n")
            json.dump(entry, f, indent=2)
            first = False
            count += 1
        f.write("\n]")

    print(f"\nWrote {count:,} studies to {parser.outdir}/sra-data.json")
