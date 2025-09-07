import ftplib
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


class SRAFileParser:
    """Class for 
    (1) Downloading SRA studies dump from FTP server
    (2) Parsing resulting XML files and extracting relevant fields
    (3) Saving to JSON
    """
    def __init__(self, tmp_dir="./tmp"):
        self.ftp_root = "ftp-trace.ncbi.nlm.nih.gov"
        self.ftp_path = "/sra/reports/Metadata"
        self.tmp_dir = Path(tmp_dir)
        self.local_dump = None

    def download_sra_from_ftp(self, dump_date):
        """Downloads SRA dump from the FTP server for the specified date
        :param dump_date: File dump extension in SRA FTP (format: YYYYMMDD)
            Tested with '20250901'
        """
        filename = f"NCBI_SRA_Metadata_{dump_date}.tar.gz"
        local_filepath = Path(self.tmp_dir, filename)

        if not self.tmp_dir.exists():
            self.tmp_dir.mkdir()

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
            if any(m.name.endswith("run.xml") for m in tar):
                for i, member in enumerate(tar):
                    if member.name.endswith(".xml"):
                        if i % 1000 == 0:
                            print(f"Processed {i:,} entries")
                        file_member = tar.extractfile(member)
                        yield file_member

    def parse_db(self):
        """Parse SRA XML files and extract relevant fields
        Write extracted data for each file type in a JSON file
        """
        data = {m: [] for m in SRAFileType}
        for fh in self.iter_sra():
            suffix = Path(fh.name).suffixes[0].lstrip('.').upper()

            try:
                file_type = SRAFileType[suffix]
            except KeyError:
                raise KeyError(f"Unknown file suffix for: {fh.name}")

            if file_type == SRAFileType.STUDY:
                entry = self.parse_study_xml(fh)
            elif file_type == SRAFileType.SAMPLE:
                entry = self.parse_sample_xml(fh)
            elif file_type == SRAFileType.EXPERIMENT:
                entry = self.parse_experiment_xml(fh)
            elif file_type == SRAFileType.RUN:
                entry = self.parse_run_xml(fh)
            elif file_type == SRAFileType.SUBMISSION:
                continue
            
            data[file_type].extend(entry)

        # Write each file type to its own JSON file
        print("Writing to JSON")
        for filetype, entries in data.items():
            if not entries:
                continue
            with open(Path(self.tmp_dir, f"{filetype.value}_data.json"), "w") as out_fh:
                json.dump(entries, out_fh, indent=2)

    def parse_study_xml(self, file_obj):
        """Parse study.xml file for a given study
        Keep following fields:
        - IDENTIFIERS/{PRIMARY_ID, EXTERNAL_IDs}
        - DESCRIPTOR/{STUDY_TITLE, STUDY_ABSTRACT, STUDY_TYPE['existing_study_type']}
        """
        data = []
        for _, elem in ET.iterparse(file_obj, events=("end",)):
            if elem.tag == "STUDY":
                # String fields
                SRP_ID = elem.findtext("IDENTIFIERS/PRIMARY_ID")
                bioproject = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='BioProject']")
                GSE_ID = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='GEO']")
                title = elem.findtext("DESCRIPTOR/STUDY_TITLE")
                abstract = elem.findtext("DESCRIPTOR/STUDY_ABSTRACT")
                
                # Dict field
                study_type_elem = elem.find("DESCRIPTOR/STUDY_TYPE")
                study_type = study_type_elem.get("existing_study_type", "") if study_type_elem is not None else ""

                # Add to JSON data
                data.append(dict(
                    SRP_ID=SRP_ID,
                    bioproject=bioproject,
                    GSE_ID=GSE_ID,
                    title=title,
                    study_type=study_type,
                    abstract=abstract
                ))
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
        data = []
        for _, elem in ET.iterparse(file_obj, events=("end",)):
            if elem.tag == "SAMPLE":
                # String fields
                SRS_ID = elem.findtext("IDENTIFIERS/PRIMARY_ID")
                biosample = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='BioSample']")
                gsm_id = elem.findtext("IDENTIFIERS/EXTERNAL_ID[@namespace='GEO']")
                title = elem.findtext("TITLE")
                species = elem.findtext("SAMPLE_NAME/SCIENTIFIC_NAME")

                # List field
                sample_attributes_elem = elem.find("SAMPLE_ATTRIBUTES") # Iterable of Tuples
                attributes = {tag.text: ' '.join(v.text for v in value) for (tag, *value) in sample_attributes_elem} if sample_attributes_elem is not None else {}

                # Add to JSON data
                data.append(dict(
                    SRS_ID=SRS_ID,
                    biosample=biosample,
                    GSM_ID=gsm_id,
                    title=title,
                    species=species,
                    sample_attributes=attributes
                ))

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
        data = []
        for _, elem in ET.iterparse(file_obj, events=("end",)):
            if elem.tag == "EXPERIMENT":
                # String fields
                SRX_ID = elem.findtext("IDENTIFIERS/PRIMARY_ID")
                SRS_ID = elem.findtext("DESIGN/SAMPLE_DESCRIPTOR/PRIMARY_ID")
                SRP_ID = elem.findtext("STUDY_REF/IDENTIFIERS/PRIMARY_ID")
                bioproject = elem.findtext("STUDY_REF/IDENTIFIERS/EXTERNAL_ID[@namespace='BioProject']")
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
                data.append(dict(
                    SRX_ID=SRX_ID,
                    SRS_ID=SRS_ID,
                    SRP_ID=SRP_ID,
                    bioproject=bioproject,
                    title=title,
                    design_description=design_descr,
                    library=library,
                    platform=platform
                ))
                elem.clear()

        return data

    def parse_run_xml(self, file_obj):
        """Parse run.xml file for a given study
        Keep following fields:
        - IDENTIFIERS/PRIMARY_ID
        - EXPERIMENT_REF['accession']
        - RUN_ATTRIBUTES
        """
        data = []
        for _, elem in ET.iterparse(file_obj, events=("end",)):
            if elem.tag == "RUN":
                # Extract relevant fields
                SRR_ID = elem.findtext("IDENTIFIERS/PRIMARY_ID")
                SRX_ID = elem.find("EXPERIMENT_REF").attrib.get("accession")
                run_attributes = elem.find("RUN_ATTRIBUTES")
                attributes = {tag.text: ' '.join(v.text or '' for v in value) for (tag, *value) in run_attributes} if run_attributes is not None else {}

                # Add to JSON data
                data.append(dict(
                    SRR_ID=SRR_ID,
                    SRX_ID=SRX_ID,
                    run_attributes=attributes
                ))
                elem.clear()

        return data


if __name__ == "__main__":
    parser = SRAFileParser()
    parser.download_sra_from_ftp(dump_date="20250901")
    data = parser.parse_db()