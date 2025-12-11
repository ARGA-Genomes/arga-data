from pathlib import Path
from lib.progressBar import ProgressBar
import logging
from lib.bigFiles import RecordWriter
from pathlib import Path
import lib.zipping as zp
import mmap
import concurrent.futures as cf
from typing import Generator

# dataClass = {
#     "EST": "expressed sequence tag",
#     "GSS": "genome survey sequence",
#     "STS": "sequence tagged site",
#     "CON": "constructed sequence",
#     "TSA": "transcriptome shotgun assembly",
#     "HTC": "high throughput assembled transcriptomic sequence",
#     "HTG": "high throughput assembled genomic sequence",
#     "STD": "standard targeted annotated assembled sequence",
#     "PAT": "sequence associated with a patent process",
#     "WGS": "whole genome shotgun contig level assembly"
# }

# taxonClass = {
#     "FUN": "Fungi",
#     "ENV": "Environmental",
#     "HUM": "Human",
#     "INV": "Invertebrate",
#     "MAM": "Mammalia",
#     "MUS": "MusMusculus",
#     "PHG": "Phage",
#     "PLN": "Plants",
#     "PRO": "Pro",
#     "ROD": "Rodent",
#     "SYN": "Synthetic",
#     "UNC": "Unclassified",
#     "VRL": "Virus",
#     "VRT": "Vertebrate",
#     "XXX": "Unknown"
# }

def parse(inputPath: Path, outputPath: Path):
    extractedFile = zp.extract(inputPath, outputPath.parent)
    if extractedFile is None:
        return
    
    _parseFile(extractedFile, outputPath)
    # extractedFile.unlink()

def _parseFile(filePath: Path, outputPath: Path):
    logging.info(f"Parsing flat file: {filePath}")

    rowsPerSubsection = 10000
    writer = RecordWriter(outputPath, rowsPerSubsection, subDirName=f"{filePath.stem}_chunks")
    skipSections = writer.writtenRecordCount()

    with open(filePath, "rb") as fp:
        lines = sum(1 for _ in fp)

    def chunkGenerator() -> Generator[str, None, None]:
        findBytes = b"\n//\n"
        with open(filePath, "rb") as fp:
            with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mfp:
                byteDiff = mfp.find(findBytes)
                while byteDiff >= 0:
                    yield mfp.read(byteDiff + len(findBytes)).decode("utf-8")
                    byteDiff = mfp.find(findBytes) - mfp.tell()

    # for chunk in chunkGenerator():
    progress = ProgressBar(lines, tasksPerUpdate=100)
    for idx, chunk in enumerate(chunkGenerator()):
        if idx >= skipSections:

            recordData = {}
            for sectionData in chunk.replace("\nXX\nXX\n", "\nXX\n").split("\nXX\n"): # Fix double XX line
                sectionHeader = sectionData[:2]
                if sectionHeader in ("AC", "AH", "KW", "CC", "CO"):
                    continue

                parsedData = _parseSection(sectionHeader, sectionData)
                if sectionHeader == "RN":
                    if "references" not in recordData:
                        recordData["references"] = []

                    recordData["references"].append(parsedData)
                else:
                    recordData |= parsedData

            writer.write(recordData)

        progress.update(chunk.count("\n"))

    writer.combine(removeParts=True)

def _parseSection(header: str, data: str) -> dict:
    def flattenNoHeader(content: str, spacer: str = " ") -> str:
        return spacer.join(line[5:] for line in content.split("\n"))
    
    def flattenNoHeaderList(content: str) -> list[str]:
        return [line[5:] for line in content.split("\n")]

    def flattenSections(content: str, spacer: str = " ", map: dict[str, str] = {}) -> dict[str, str]:
        res = {}
        for line in content.split("\n"):
            code = line[:2]
            lineData = line[5:]

            if code not in res:
                res[code] = lineData
            else:
                res[code] += f"{map.get(code, spacer)}{lineData}"
        
        return res

    if header == "ID":
        accession, _, topology, mol_type, dataClass, tax_division, base_count = data.rstrip("\n").split("; ")
        return {
            "sequence": accession,
            "topology": topology,
            "mol_type": mol_type,
            "dataclass": dataClass,
            "tax_division": tax_division,
            "base_count": int(base_count[:-4]) # Clean off " BP."
        }

    elif header == "PR":
        projects = {}
        for line in data.split("\n"):
            key, value = line[5:].rstrip(";").split(":", 1)
            projects[key.lower()] = value

        return projects

    elif header == "DT":
        originalDate, date = data.split("\n")
        return {
            "original_date": originalDate[5:],
            "date": date[5:]
        }

    elif header == "DE":
        return {"description": flattenNoHeader(data)}

    elif header == "OS":
        splitData = data.split("OG   ")
        scientificName, lineage = splitData[0].split("\n", 1)
        taxonData = {
            "scientific_name": scientificName[5:],
            "lineage": flattenNoHeader(lineage)
        }

        if len(splitData) > 1: # OG section exists
            taxonData["sample"] = splitData[1]

        return taxonData

    elif header == "RN":
        referenceData = flattenSections(data, " ", {"RX": "\n"})
        referenceData["pages"] = referenceData.pop("RP", "")
        referenceData["comment"] = referenceData.pop("RC", "")

        for externalResource in referenceData.pop("RX", "").split("\n"):
            if not externalResource: # Split creates list with empty string
                continue

            link, value = externalResource[:-1].split("; ", 1)
            referenceData[link.lower()] = value

        referenceData["group"] = referenceData.pop("RG", "")
        referenceData["authors"] = referenceData.pop("RA", "").rstrip(";")
        referenceData["title"] = referenceData.pop("RT", "").strip("\";")
        referenceData["literature"] = referenceData.pop("RL", "")

        return referenceData

    elif header == "DR":
        dataReferences = {}
        for reference in flattenNoHeaderList(data):
            refName, value = reference.split("; ", 1)
            if refName not in dataReferences:
                dataReferences[refName] = [value[:-1]]
            else:
                dataReferences[refName].append(value[:-1])

        return {"data_references": dataReferences}

    elif header == "FH":
        def parseLine(line: str) -> tuple[str, str]:
            if "=" not in line:
                return "other", line
            
            key, value = line.split("=", 1)
            return key, value.strip("\"")

        currentHeader = ""
        currentBP = ""
        newHeader = ""
        newBP = ""

        features = {}
        for line in flattenNoHeader(data.split("\n", 1)[-1], "\n").split("\n" + 16*" " + "/"):
            line = line.replace("\n" + " "*16, " ") # Flatten lines that bleed over

            # Handling upcoming header
            if "\n" in line: # New section
                splitLines = line.split("\n")
                line = splitLines[0] # Leftover line data
                newHeader = splitLines[-1] # Last section is new header, anything in between only have a header with BP data and can be ignored
                
                newHeader, newBP = newHeader.rsplit(" ", 1)
                newHeader = newHeader.strip() # Clean out extra whitespace after text

                if newBP not in features:
                    features[newBP] = {}

                if newHeader not in features[newBP]:
                    features[newBP][newHeader] = {}

            # Handline current line data
            if line and currentHeader != "translation":
                key, value = parseLine(line)
                if key not in features[currentBP][currentHeader]:
                        features[currentBP][currentHeader][key] = ""

                features[currentBP][currentHeader][key] += value
                
            if newHeader:
                currentHeader = newHeader
                currentBP = newBP
                newHeader = ""

        # Combine raw source information into dict
        fullRange = list(features.keys())[0]
        sourceInfo = features[fullRange].pop("source")
        features = sourceInfo | features
        return {"features_genes": str(features)}

    elif header == "SQ":
        return {"sequence_info": data}

    else:
        print(f"UNHANDLED header: {header}")
