from pathlib import Path
from enum import Enum
import pandas as pd

class Section(Enum):
    INFO = "INFO"
    REFERENCE = "REFERENCE"
    COMMENT = "COMMENT"
    FEATURES = "FEATURES"
    ORIGIN = "ORIGIN"

_seqBaseURL = "https://ftp.ncbi.nlm.nih.gov/genbank/"
_genbankBaseURL = "https://www.ncbi.nlm.nih.gov/nuccore/"
_fastaSuffix = "?report=fasta&format=text"

def parseFlatfile(filePath: Path, verbose: bool = False) -> pd.DataFrame:
    with open(filePath) as fp:
        try:
            data = fp.read()
        except UnicodeDecodeError:
            print(f"Failed to read file: {filePath}")
            return []

    # Cut off header of file
    firstLocusPos = data.find("LOCUS")
    header = data[:firstLocusPos]
    data = data[firstLocusPos:]

    records = []
    for idx, entry in enumerate(data.split("//\n")[:-1], start=1): # Split into separate loci, exclude empty entry after last
        if verbose:
            print(f"Parsing entry: {idx}", end="\r")

        entryData = _parseEntry(entry)

        # Attach seq file path and fasta file
        entryData["seq_file"] = f"{_seqBaseURL}{filePath.name}.gz"
        version = entryData.get("version", "")
        if version:
            entryData["genbank_url"] = f"{_genbankBaseURL}{version}"
            entryData["fasta_url"] = f"{_genbankBaseURL}{version}{_fastaSuffix}"

        # Add specimen field
        for value in (entryData.get("specimen_voucher", None), entryData.get("isolate", None), f"NCBI_{entryData['accession']}_specimen"):
            if value is not None:
                entryData["specimen"] = value
                break

        records.append(entryData)

    if verbose:
        print()

    return pd.DataFrame.from_records(records)
    
def _parseEntry(entryBlock: str) -> dict:
    sections = _getSections(entryBlock, allowDigits=False)

    subParsers = {
        "LOCUS": _parseLocus,
        "DEFINITION": _parseText,
        "ACCESSION": _parseText,
        "VERSION": _parseText,
        "COMMENT": _parseText,
        "DBLINK": _parseDBs,
        "KEYWORDS": _parseKeywords,
        "SOURCE": _parseSource,
        "REFERENCE": _parseReferences,
        "FEATURES": _parseFeatures,
        "ORIGIN": _parseOrigin,
        "CONTIG": _parseOrigin
    }

    entryData = {}
    for section in sections:
        heading, data = section.split(" ", 1)
        # headings = ["LOCUS", "DEFINITION", "ACCESSION", "VERSION", "DBLINK", "KEYWORDS", "SOURCE", "ORGANISM"]
        
        subParser = subParsers.get(heading, None)
        if subParser is None:
            print(f"Unhandled heading: {heading}")
            continue

        subParser(heading, data, entryData)
    
    return entryData

def _parseLocus(heading: str, data: str, entryData: dict) -> None:
        parameters = ["locus", "basepairs", "", "type", "shape", "seq_type", "date"]
        entryData |= {param: value.strip() for param, value in zip(parameters, data.split()) if param}

def _parseText(heading: str, data: str, entryData: dict) -> None:        
    entryData[heading.lower()] = _flattenBlock(data)

def _parseDBs(heading: str, data: str, entryData: dict) -> None:
    dbs = {}
    key = "unknown_db"

    for line in data.split("\n"):
        if not line.strip():
            continue

        if ":" in line: # Line has a new key in it
            key, line = line.split(":")
            key = key.strip().lower()
            dbs[key] = []

        if key == "unknown_db": # Found a line before a db name
            dbs[key] = [] # Create a list for values with no db name
            
        dbs[key].extend([element.strip() for element in line.split(",")])

    entryData |= dbs

def _parseKeywords(heading: str, data: str, entryData: dict) -> None:
    if data.strip() == ".":
        entryData["keywords"] = ""
    else:
        entryData["keywords"] = _flattenBlock(data)

def _parseSource(heqading: str, data: str, entryData: dict) -> None: 
    source, leftover = _getSections(data, 2)
    organism, higherClassification = leftover.split("\n", 1)

    entryData["source"] = source.strip()
    entryData["organism"] = organism.split(" ", 1)[1].strip()
    entryData["higher_classification"] = _flattenBlock(higherClassification)

def _parseReferences(heading: str, data: str, entryData: dict) -> None:
    reference = {}
    refInfo = _getSections(data, 2)
    basesInfo = refInfo.pop(0)
    basesInfo = basesInfo.strip(" \n").split(" ", 1)

    if len(basesInfo) == 1: # Only reference number
        reference["bases"] = []
    elif "bases" not in basesInfo[1]: # Bases not specified
        reference["bases"] = []
    else:
        baseRanges = basesInfo[1].strip().lstrip("(bases").rstrip(")")
        bases = []
        for baseRange in baseRanges.split(";"):
            if not baseRange:
                continue

            try:
                basesFrom, basesTo = baseRange.split("to")
                bases.append(f"({basesFrom.strip()}) to {basesTo.strip()}")
            except:
                print(f"{data}, ERROR: {baseRange}")

        reference["bases"] = bases

    for section in refInfo:
        sectionName, sectionData = section.strip().split(" ", 1)
        if sectionName == "JOURNAL":
            if "PUBMED" in sectionData:
                sectionData, pubmed = sectionData.split("PUBMED")
                reference["journal"] = _flattenBlock(sectionData)
                reference["pubmed"] = pubmed.strip()

        reference[sectionName.lower()] = _flattenBlock(sectionData)

    if "references" not in entryData:
        entryData["references"] = []

    entryData["references"].append(reference)

def _parseFeatures(heading: str, data: str, entryData: dict) -> None:
    featureBlocks = _getSections(data, 5)
    genes = {}
    otherProperties = {}

    for block in featureBlocks[1:]:
        sectionHeader, sectionData = block.lstrip().split(" ", 1)

        propertyList = _flattenBlock(sectionData, "//").split("///")
        properties = {"bp_range": propertyList[0]}
        for property in propertyList[1:]: # base pair range is first property in list
            splitProperty = property.replace("//", "").split("=")
            if len(splitProperty) == 2:
                key, value = splitProperty
            else:
                key = splitProperty[0]
                value = key

            properties[key.strip('"')] = value.strip('"')

        if sectionHeader == "source": # Break out source section into separate columns
            organism = properties.pop("organism", None)
            if organism is not None:
                properties["features_organism"] = organism

            pcrPrimers = properties.pop("PCR_primers", None)
            if pcrPrimers is not None:
                pcrSections = pcrPrimers.split(",")

                for section in pcrSections:
                    if "_" not in section:
                        properties["PCR_primers"] = section
                        break

                    key, value = section.split(":")
                    properties[key.strip()] = value.strip()

            entryData |= properties

        else:
            gene = properties.get("gene", None)
            if gene is not None:  # If section is associated with a gene
                properties.pop("gene")
                if gene not in genes:
                    genes[gene] = {}

                if sectionHeader not in genes[gene]:
                    genes[gene][sectionHeader] = []

                genes[gene][sectionHeader].append(properties)

            else: # Not associated with a gene
                if sectionHeader not in otherProperties:
                    otherProperties[sectionHeader] = []

                otherProperties[sectionHeader].append(properties)
    
    # Only write genes and other properties if exists
    if genes:
        entryData["genes"] = genes

    if otherProperties:
        entryData["other_properties"] = otherProperties

def _parseOrigin(header: str, data: str, entryData: dict) -> None:
    pass

def _getSections(textBlock: str, whitespace: int = 0, allowDigits=True) -> list[str]:
    sections = []
    sectionStart = 0
    searchPos = 0
    textBlock = textBlock.rstrip("\n") # Make sure block doesn't end with newlines

    while True:
        nextNewlinePos = textBlock.find("\n", searchPos)
        nextPlus1 = nextNewlinePos + 1

        if nextNewlinePos < 0: # No next new line
            sections.append(textBlock[sectionStart:])
            break

        if len(textBlock[nextPlus1:nextPlus1+whitespace+1].strip()) == 0: # No header on next line
            searchPos = nextPlus1
            continue

        if not allowDigits and textBlock[nextPlus1+whitespace+1].isdigit(): # Check for digit leading next header
            searchPos = nextPlus1
            continue

        sections.append(textBlock[sectionStart:nextNewlinePos])
        sectionStart = searchPos = nextPlus1

    return sections
    
def _flattenBlock(textBlock: str, joiner: str = " ") -> str:
    return joiner.join(line.strip() for line in textBlock.split("\n") if line)
