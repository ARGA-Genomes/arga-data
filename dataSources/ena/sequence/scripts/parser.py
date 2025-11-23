from pathlib import Path
import pandas as pd
from lib.progressBar import ProgressBar
import logging
import traceback
from typing import Generator

def parseFile(filePath: Path) -> pd.DataFrame | None:
    logging.info(f"Parsing flat file: {filePath}")

    with open(filePath) as fp:
        sections = fp.read().split("//")

    # Iterate through sections of file
    progress = ProgressBar(len(sections))
    records = []
    for section in sections:
        data = {}
        repeatCount = 1

        for line in section.split("\n"):

            if line[:2] == code:
                repeatCount += 1
            else:
                code = line[:2]
                repeatCount = 1

            if code == "XX" or code == "  ":
                continue

            lineData = line[5:]
            if code == "ID":
                sequence, _, topology, mol_type, dataclass, tax_division, base_count = lineData.split("; ")
                data["sequence"] = sequence
                data["topology"] = topology
                data["mol_type"] = mol_type
                data["dataclass"] = dataclass
                data["tax_division"] = tax_division
                data["base_count"] = int(base_count[:-4]) # Clean off " BP."

            elif code == "AC":
                data["accession"] = lineData

            elif code == "DT":
                ...

            elif code == "DE":
                ...

            elif code == "KW":
                ...

            elif code == "OS":
                ...

            elif code == "OC":
                ...

            elif code == "RN":
                ...

            elif code == "RP":
                ...

            elif code == "RX":
                ...

            elif code == "RA":
                ...

            elif code == "RT":
                ...

            elif code == "RL":
                ...

            elif code == "DR":
                ...

            elif code == "CC":
                ...

            elif code == "FH":
                ...

            elif code == "FT":
                ...

            elif code == "SQ":
                ...

            else:
                print(f"UNHANDLED CODE: {code}")

        progress.update()

    return pd.DataFrame.from_records(records)

# def commentParser(self, data: str):
#     self.data["comment"] = "\\n".join(self.getSections(f"{7*' '}{data}", 12, flattenLines=True))

#     parseCommentTags = [
#         "Genome-Assembly-Data",
#         "Genome-Annotation-Data",
#         "Assembly-Data"
#     ]

#     for tag in parseCommentTags:
#         startTag = f"##{tag}-START##"
#         endTag = f"##{tag}-END##"

#         startPos = data.find(startTag)
#         if startPos < 0: # Didn't find tag
#             continue

#         mapping = data[startPos+len(startTag):data.find(endTag)].strip()
#         mapping = self.getSections(mapping, 12, flattenLines=True)
#         for item in mapping:
#             key, value = item.split("::")
#             self.data[key.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")] = value.strip()

# def dblinkParser(self, data: str):
#     dbs = self.getSections(f"{7*' '}{data}", 12, flattenLines=True)
    
#     cleanedDBs: list[str] = []
#     for db in dbs:
#         if ":" not in db:
#             cleanedDBs[-1] += f" {db}"
#         else:
#             cleanedDBs.append(db)

#     baseURLS = {
#         "BioProject": "https://www.ncbi.nlm.nih.gov/bioproject/",
#         "BioSample": "https://www.ncbi.nlm.nih.gov/biosample/",
#         "Sequence Read Archive": "https://www.ncbi.nlm.nih.gov/sra/",
#         "ProbeDB": "https://www.ncbi.nlm.nih.gov/biosample/",
#         "Assembly": "https://www.ncbi.nlm.nih.gov/assembly/",
#         "Project": "https://www.ncbi.nlm.nih.gov/bioproject/"
#     }

#     for db in cleanedDBs:
#         dbName, dbCodes = db.split(":")
#         lowerName = dbName.lower()

#         self.data[lowerName] = []
#         for dbCode in dbCodes.split(","):
#             self.data[lowerName].append(baseURLS.get(dbName) + dbCode.strip())

# def keywordsParser(self, data: str):
#     self.data["keywords"] = "" if data.strip() == "." else self.flattenLines(data)

# def sourceParser(self, data: str):
#     source, remainder = self.getSections(data, 2)
#     organism, higherClassification = remainder.split("\n", 1)

#     self.data["source"] = source.strip()
#     self.data["organism"] = organism.strip().split(" ", 1)[1].strip()
#     self.data["higher_classification"] = self.flattenLines(higherClassification)

# def referenceParser(self, data: str):
#     references = "references"
#     if references not in self.data:
#         self.data[references] = []

#     referenceItems = self.getSections(data, 2, flattenLines=True)
#     referenceProperties = {}

#     for idx, item in enumerate(referenceItems):
#         if idx == 0: # Reference number + base pair range
#             bpRange = item.split(" ", 1)[-1]
#             referenceProperties["bp_range"] = bpRange
#             continue
        
#         key, value = item.split(" ", 1)
#         referenceProperties[key.lower()] = value.strip()

#     self.data[references].append(referenceProperties)

# def featuresParser(self, data: str):
#     extraFeatures = "other"

#     def linesToDict(lines: list[str]) -> dict:
#         # Clean up leading / and merge lines without leading /
#         cleanLines = []
#         for line in lines:
#             if line.startswith("/"):
#                 cleanLines.append(line[1:])
#             elif not cleanLines:
#                 cleanLines.append(line)
#             else:
#                 cleanLines[-1] += f" {line}"

#         # Parse cleaned lines to split key/value pairs
#         retVal = {}
#         for line in cleanLines:
#             if "=" not in line:
#                 if extraFeatures not in retVal:
#                     retVal[extraFeatures] = []

#                 retVal[extraFeatures].append(line)
#                 continue

#             key, value = line.split("=", 1)
#             retVal[key] = value.strip('"')

#         return retVal

#     data = data.split("\n", 1)[-1]
#     featureBlocks = self.getSections(data, 5)

#     genesLabel = "features_genes"
#     self.data[genesLabel] = {}
#     for block in featureBlocks:
#         blockHeader, blockData = block.lstrip().split(" ", 1)

#         if "\n" not in blockData: # No properties after base pair range
#             bpRange = blockData.strip()
#             properties = {}
#         else: # Parse properties
#             bpRange, properties = blockData.lstrip().split("\n", 1)
#             sections = self.getSections(properties, 21, flattenLines=True)
#             properties = linesToDict(sections)

#         if blockHeader == "source": # Source properties get split out and put directly into data
#             properties["features_other"] = properties.pop(extraFeatures, [])
#             properties["features_organism"] = properties.pop("organism", "")
#             self.data |= properties
#             continue

#         if bpRange not in self.data[genesLabel]:
#             self.data[genesLabel][bpRange] = {}

#         properties.pop("translation", None) # Remove translation
#         self.data[genesLabel][bpRange][blockHeader] = properties

#     self.data[genesLabel] = str(self.data[genesLabel])
