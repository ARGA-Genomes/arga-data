from pathlib import Path
import pandas as pd
from lib.progressBar import ProgressBar
import logging
import traceback
from typing import Generator

def parseFlatfile(filePath: Path) -> pd.DataFrame | None:
    logging.info(f"Parsing flat file: {filePath}")

    def sectionGenerator() -> Generator[str, None, None]:
        val = ""
        skipping = False
        with open(filePath) as fp:
            for line in fp:
                if not line[0].isspace(): # New section hit, yield previous
                    if skipping:
                        skipping = False
                    else:
                        yield val
                        val = ""

                    if line.startswith("ORIGIN"): # Start skipping next value
                        skipping = True

                if not skipping:
                    val += line
        
        yield val # Yield final section after last origin
        return

    iterator = sectionGenerator()

    # Get header data
    _, headerData = [next(iterator) for _ in range(2)]
    headerData = headerData.split("\n")
    fileName = headerData[0].split(" ", 1)[0]
    date = headerData[1]
    releaseNum = headerData[3].rsplit(" ", 1)[-1]
    loci = headerData[7].lstrip().split(" ", 1)[0]
    headerData = {"filename": fileName.lower(), "date": date.strip(), "release_num": releaseNum, "seq_file": f"https://ftp.ncbi.nlm.nih.gov/genbank/{fileName}.gz"}

    # Iterate through rest of file
    progress = ProgressBar(int(loci))
    records = []
    currentEntry = Entry(headerData)
    for sectionData in iterator:
        if sectionData == "//\n": # End of entry
            records.append(currentEntry)
            progress.update(extraInfo=f"{len(records)}/{loci} loci")
            currentEntry = Entry(headerData)
            continue

        heading, sectionData = sectionData.split(" ", 1)
        currentEntry.addSection(heading, sectionData)

    df = pd.DataFrame.from_records([entry.data for entry in records])
    df["specimen"] = ""
    for column in ("specimen_voucher", "isolate", "accession"):
        if column in df.columns:
            df["specimen"] = df["specimen"].fillna(df[column])

    return df

class Entry:

    __slots__ = "data"

    def __init__(self, headerData: dict):
        self.data = dict(headerData)
        
    def addSection(self, heading: str, data: str) -> None:
        parser = getattr(self, f"{heading.lower()}Parser", None)
        if parser is None:
            return
        
        try:
            parser(data)
        except:
            print(f"\nFailed to parse {heading}")
            print(traceback.format_exc())
            return

    def flattenLines(self, text: str, joiner: str = " ") -> str:
        return joiner.join(line.strip() for line in text.split("\n") if line)

    def getSections(self, text: str, leadingWhiteSpace: int = 0, allowLeadingDigits: bool = True, flattenLines: bool = False) -> list[str]:
        sections = []
        lines = text.split("\n")
        
        for line in lines:
            if not line:
                continue

            if not sections:
                sections.append(line.strip() if flattenLines else line)
                continue

            if line[leadingWhiteSpace] != " " and all(char == " " for char in line[:leadingWhiteSpace]): # Valid section start line
                if allowLeadingDigits or line[leadingWhiteSpace].isdigit(): # Leading character is valid for section start
                    sections.append(line.strip() if flattenLines else line)
                    continue

            sections[-1] += f" {line.strip()}" if flattenLines else f"\n{line}"

        return sections
    
    def locusParser(self, data: str):
        locusPropertyNames = ["locus", "base_pairs", None, "type", "shape", "seq_type", "date"] # None entry to negate "bp" text
        for propertyName, value in zip(locusPropertyNames, data.split()):
            if propertyName is not None:
                self.data[propertyName] = value

    def definitionParser(self, data: str):
        self.data["definition"] = self.flattenLines(data)

    def accessionParser(self, data: str):
        self.data["accession"] = data.strip()

    def versionParser(self, data: str):
        genbankURL = "https://www.ncbi.nlm.nih.gov/nuccore/"
        fastaSuffix = "?report=fasta&format=text"

        version = data.strip()
        self.data["version"] = version
        self.data["genbank_url"] = f"{genbankURL}{version}" 
        self.data["fasta_url"] = f"{genbankURL}{version}{fastaSuffix}"
    
    def commentParser(self, data: str):

        def getTags(section: str) -> list[str, str]:
            return [f"##Genome-{section}-Data-{position}##" for position in ("START", "END")]

        self.data["comment"] = data.replace("\n", " ")
        for section in ("Assembly", "Annotation"):
            startTag, endTag = getTags(section)
            startPos = data.find(startTag)
            if startPos < 0: # Didn't find tag
                continue

            mapping = data[startPos+len(startTag):data.find(endTag)].strip()
            mapping = self.getSections(mapping, 12, flattenLines=True)
            for item in mapping:
                key, value = item.split("::")
                self.data[key.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")] = value.strip()

    def dblinkParser(self, data: str):
        dbs = self.getSections(f"{7*' '}{data}", 12, flattenLines=True)
        
        cleanedDBs: list[str] = []
        for db in dbs:
            if ":" not in db:
                cleanedDBs[-1] += f" {db}"
            else:
                cleanedDBs.append(db)

        baseURLS = {
            "BioProject": "https://www.ncbi.nlm.nih.gov/bioproject/",
            "BioSample": "https://www.ncbi.nlm.nih.gov/biosample/",
            "Sequence Read Archive": "https://www.ncbi.nlm.nih.gov/sra/",
            "ProbeDB": "https://www.ncbi.nlm.nih.gov/biosample/",
            "Assembly": "https://www.ncbi.nlm.nih.gov/assembly/"
        }

        for db in cleanedDBs:
            dbName, dbCodes = db.split(":")
            lowerName = dbName.lower()

            self.data[lowerName] = []
            for dbCode in dbCodes.split(","):
                self.data[lowerName].append(baseURLS.get(dbName) + dbCode.strip())

    def keywordsParser(self, data: str):
        self.data["keywords"] = "" if data.strip() == "." else self.flattenLines(data)

    def sourceParser(self, data: str):
        source, remainder = self.getSections(data, 2)
        organism, higherClassification = remainder.split("\n", 1)

        self.data["source"] = source.strip()
        self.data["organism"] = organism.strip().split(" ", 1)[1].strip()
        self.data["higher_classification"] = self.flattenLines(higherClassification)

    def referenceParser(self, data: str):
        references = "references"
        if references not in self.data:
            self.data[references] = []

        referenceItems = self.getSections(data, 2, flattenLines=True)
        referenceProperties = {}

        for idx, item in enumerate(referenceItems):
            if idx == 0: # Reference number + base pair range
                bpRange = item.split(" ", 1)[-1]
                referenceProperties["bp_range"] = bpRange
                continue
            
            key, value = item.split(" ", 1)
            referenceProperties[key.lower()] = value.strip()

        self.data[references].append(referenceProperties)

    def featuresParser(self, data: str):
        extraFeatures = "other"

        def linesToDict(lines: list[str]) -> dict:
            retVal = {}
            for line in lines:
                if "=" not in line:
                    if extraFeatures not in retVal:
                        retVal[extraFeatures] = []

                    retVal[extraFeatures].append(line)
                    continue

                key, value = line.split("=", 1)
                retVal[key[1:]] = value.strip('"')

            return retVal

        data = data.split("\n", 1)[-1]
        featureBlocks = self.getSections(data, 5)

        genesLabel = "features_genes"
        self.data[genesLabel] = {}
        for block in featureBlocks:
            blockHeader, blockData = block.lstrip().split(" ", 1)
            if "\n" not in blockData: # No properties after base pair range
                self.data[genesLabel][blockData.strip()] = blockHeader
                continue

            bpRange, properties = blockData.lstrip().split("\n", 1)
            properties = linesToDict(self.getSections(properties, 21, flattenLines=True))

            if blockHeader == "source":
                properties["features_other"] = properties.pop(extraFeatures, [])
                properties["features_organism"] = properties.pop("organism", "")
                self.data |= properties
                continue

            if bpRange not in self.data[genesLabel]:
                self.data[genesLabel][bpRange] = {}

            properties.pop("translation", None) # Remove translation
            self.data[genesLabel][bpRange][blockHeader] = properties

        self.data[genesLabel] = str(self.data[genesLabel])
