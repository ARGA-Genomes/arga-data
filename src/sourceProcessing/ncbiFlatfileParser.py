from pathlib import Path
from enum import Enum
import pandas as pd

class Section(Enum):
    LOCUS = "LOCUS"
    DEFINITION = "DEFINITION"
    ACCESSION = "ACCESSION"
    VERSION = "VERSION"
    COMMENT = "COMMENT"
    DBLINK = "DBLINK"
    KEYWORDS = "KEYWORDS"
    SOURCE = "SOURCE"
    REFERENCE = "REFERENCE"
    FEATURES = "FEATURES"
    ORIGIN = "ORIGIN"
    CONTIG = "CONTIG"
    
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
        specimenOptions = [
            "specimen_voucher"
            "isolate"
            "accession"
        ]

        for idx, option in enumerate(specimenOptions, start=1):
            value = entryData.get(option, None)
            if value is not None:
                if idx == len(specimenOptions):
                    value = f"NCBI_{value}_specimen"
                entryData["specimen"] = value
                break
        else: # No specimen set
            entryData["specimen"] = None

        records.append(entryData)

    if verbose:
        print()

    return pd.DataFrame.from_records(records)
    
def _parseEntry(entryBlock: str) -> dict:
    splitSections = _getSections(entryBlock, allowDigits=False)

    entryData = {}
    for sectionBlock in splitSections:
        heading, data = sectionBlock.split(" ", 1)

        if heading not in Section._value2member_map_:
            print(f"Unhandled heading: {heading}")
            continue

        section = Section(heading)
        if section == Section.LOCUS:
            entryData |= _parseLocus(data)

        elif section in (Section.DEFINITION, Section.ACCESSION, Section.VERSION, Section.COMMENT):
            entryData |= _parseText(heading, data)

        elif section == Section.DBLINK:
            entryData |= _parseDBs(data)

        elif section == Section.KEYWORDS:
            entryData |= _parseKeywords(data)

        elif section == Section.SOURCE:
            entryData |= _parseSource(data)

        elif section == Section.REFERENCE:
            extract = {
                "pubmed": "",
                "title": "",
                "authors": [],
                "journal": "",
                "bases": [],
                "remark": ""
            }

            if "references" not in entryData:
                entryData["references"] = []
                for key in extract:
                    entryData[key] = []

            reference, extracted = _parseReference(data, extract)
            entryData["references"].append(reference)
            for key, value in extracted.items():
                entryData[key].append(value)

        elif section == Section.FEATURES:
            entryData |= _parseFeatures(data)

        elif section == Section.ORIGIN:
            pass

        elif section == Section.CONTIG:
            pass

    # Stringify columns so they can be saved as parqet/csv
    stringColumns = ["authors", "bases"]
    for column in stringColumns:
        entryData[column] = str(entryData[column])

    return entryData

def _parseLocus(data: str) -> dict[str, str]:
        parameters = ["locus", "basepairs", "", "type", "shape", "seq_type", "date"]
        return {param: value.strip() for param, value in zip(parameters, data.split()) if param}

def _parseText(heading: str, data: str) -> dict[str, str]:  
    return {heading.lower(): _flattenBlock(data)}

def _parseDBs(data: str) -> dict[str, str]:
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

    return dbs

def _parseKeywords(data: str) -> dict[str, str]:
    return {"keywords": "" if data.strip() == "." else _flattenBlock(data)}

def _parseSource(data: str) -> dict[str, str]: 
    source, leftover = _getSections(data, 2)
    organism, higherClassification = leftover.split("\n", 1)
    return {"source": source.strip(), "organism": organism.split(" ", 1)[1].strip(), "higher_classification": _flattenBlock(higherClassification)}

def _parseReference(data: str, extract: dict) -> tuple[dict[str, str], dict[str, str | list]]:
    reference = {}
    refInfo = _getSections(data, 2)
    basesInfo = refInfo.pop(0)
    basesInfo = basesInfo.strip(" \n").split(" ", 1)
    # reference["id"] = int(basesInfo[1].strip())

    if len(basesInfo) == 1 or "bases" not in basesInfo[1]: # Only reference number or bases not specified
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

    # Split reference after logic
    extracted = {}
    for key, default in extract.items():
        extracted[key] = reference.get(key, default)

    return reference, extracted

def _parseFeatures(data: str) -> dict[str, str]:
    featureBlocks = _getSections(data, 5)
    genes = {}
    otherProperties = {}

    features = {}
    for block in featureBlocks[1:]:
        sectionHeader, sectionData = block.lstrip().split(" ", 1)

        propertyList = _flattenBlock(sectionData, "//").split("///") # Single slashes exist in data, convert newlines to 2 slashes and then split on 3
        properties = {"bp_range": propertyList[0]}
        for property in propertyList[1:]: # base pair range is first property in list
            splitProperty = property.replace("//", " ").split("=")
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

            features |= properties

        else: # Every other block that isn't source
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
        features["genes"] = genes

    if otherProperties:
        features["other_properties"] = otherProperties

    return features

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
