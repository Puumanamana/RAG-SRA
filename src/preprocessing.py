import xml.etree.ElementTree as ET
import requests
from pathlib import Path
import json
import html
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
import warnings

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)


def download_bioprojects(output="./rag-data/bioproject.xml"):
    """Download the Bioproject XML data from NCBI."""
    output = Path(output)

    if not output.parent.exists():
        output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        print(f"Using existing {output}")
    else:
        print(f"Downloading {output}")
        url = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"
        response = requests.get(url)
        response.raise_for_status()
        with open(output, "wb") as fh:
            fh.write(response.content)
    return output


def parse_project(elem):
    """Extract all relevant information from a <Project> element into a dict."""
    # Project identifiers
    archive = elem.find("ProjectID/ArchiveID")
    accession = archive.get("accession") if archive is not None else None

    # Basic description
    descr = elem.find("ProjectDescr")
    name = descr.findtext("Name") if descr is not None else None
    title = descr.findtext("Title") if descr is not None else None
    description = descr.findtext("Description") if descr is not None else None
    if description:
        description = html.unescape(description)
        soup = BeautifulSoup(description, "html.parser")
        description = soup.get_text(separator=" ", strip=True)

    # Publications
    publications = []
    for pub in elem.findall(".//Publication"):
        citation = pub.find("StructuredCitation")
        publications.append({
            "title": citation.findtext("Title") if citation is not None else None,
            "journal": citation.findtext("Journal/JournalTitle") if citation is not None else None,
            "year": citation.findtext("Journal/Year") if citation is not None else None,
        })

    # Organism info
    organism = elem.findtext(".//OrganismName")

    diseases = [x.text for x in elem.findall(".//Disease")]

    return {
        "accession": accession,
        "name": name,
        "title": title,
        "description": description,
        "publications": publications,
        "organism": organism,
        "diseases": diseases,
    }


if __name__ == "__main__":
    fpath = download_bioprojects()

    # Parse all projects
    print("Parsing bioprojects")
    data = []
    with open(fpath, "r") as fh:
        for _, elem in ET.iterparse(fh, events=("end",)):
            # Each package is a separate project, relevant data is nested within
            # <Package><Project><Project>...</Project></Project></Package>
            if elem.tag == "Package":
                elem = elem.find("Project/Project")
                project_data = parse_project(elem)
                data.append(project_data)
                elem.clear()

    print("Saving to JSON")
    with open(fpath.with_suffix(".json"), "w") as fh:
        json.dump(data, fh, indent=2)