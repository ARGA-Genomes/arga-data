from pathlib import Path
from lib.progressBar import ProgressBar
import logging
from lib.bigFiles import RecordWriter
from pathlib import Path
import lib.zipping as zp
import mmap

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
    extractedFile.unlink()

def _parseFile(filePath: Path, outputPath: Path):
    logging.info(f"Parsing flat file: {filePath}")

    rowsPerSubsection = 30000
    writer = RecordWriter(outputPath, rowsPerSubsection, subDirName=f"{filePath.stem}_chunks")
    skipSections = writer.writtenRecordCount()

    sectionCount = 0
    offset = 0
    lineCount = 0
    lineOffset = 0
    with open(filePath, "rb") as fp:
        with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mfp:
            for line in iter(mfp.readline, b""):
                lineCount += 1
                if line == b"//\n":
                    sectionCount += 1
                    if sectionCount < skipSections:
                        offset = mfp.tell()
                        lineOffset = lineCount

    logging.info(f"Found {sectionCount} sections in file")
    progress = ProgressBar(lineCount - lineOffset, callsPerUpdate=1000)
    with open(filePath, "rb") as fp:
        with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mfp:
            mfp.seek(offset)

            sectionData = {}
            lastCode = ""
            subsectionData = ""

            for line in iter(mfp.readline, b""):
                code = line[:2]
                lineData = line[5:].decode("utf-8")
                if code == b"//": # Section break
                    writer.write(sectionData)
                    sectionData = {}
                    subsectionData = ""
                elif code == lastCode:
                    subsectionData += lineData
                else: # XX will automatically trigger
                    if subsectionData:
                        updateSectionData(sectionData, lastCode, subsectionData)

                    subsectionData = lineData

                lastCode = code
                progress.update()

    writer.combine(removeParts=True)

def updateSectionData(currentData: dict, code: bytes, data: str) -> None:
    if code in (b"XX", b"  ", b"KW", b"CO", b"AS", b"AH", b"CC", b"FH"):
        return
    
    data = data.rstrip("\n")
    if code == b"ID":
        sequence, _, topology, mol_type, dataClass, tax_division, base_count = data.split("; ")

        currentData.update({
            "sequence": sequence,
            "topology": topology,
            "mol_type": mol_type,
            "dataclass": dataClass,
            "tax_division": tax_division,
            "base_count": int(base_count[:-4]) # Clean off " BP."
        })

    elif code == b"AC":
        currentData["accession"] = data.rstrip(";")
    
    elif code == b"PR":
        key, value = data[:-1].split(":", 1)
        currentData[key.lower()] = value

    elif code == b"DT":
        originalDate, date = data.split("\n")
        currentData.update({
            "original_date": originalDate,
            "date": date
        })

    elif code == b"DE":
        currentData["description"] = data.replace("\n", " ")

    elif code == b"OS":
        currentData["scientific_name"] =data

    elif code == b"OC":
        currentData["lineage"] = data.replace("\n", " ")

    elif code == b"OG":
        currentData["sample"] = data

    elif code == b"RN":
        if "references" not in currentData:
            currentData["references"] = []
        
        currentData["references"].append({})

    elif code == b"RP":
        currentData["references"][-1]["base_range"] = data

    elif code == b"RC":
        currentData["references"][-1]["comment"] = data.replace("\n", " ")

    elif code == b"RX":
        link, value = data[:-1].split("; ", 1)
        currentData["references"][-1][link.lower()] = value

    elif code == b"RA":
        currentData["references"][-1]["authors"] = data.replace("\n", " ").rstrip(";")

    elif code == b"RT":
        currentData["references"][-1]["title"] = data.replace("\n", " ").strip("\";")

    elif code == b"RL":
        currentData["references"][-1]["literature"] = data[:-1]

    elif code == b"RG":
        currentData["references"][-1]["group"] = data

    elif code == b"DR":
        currentData["data_references"] = {}
        for reference in data.split("\n"):
            key, value = reference.split("; ", 1)
            
            if key not in currentData["data_references"]:
                currentData["data_references"] = value
            else:
                currentData["data_references"] += f", {value}"

    elif code == b"FT":
        features = {}

        for line in data.split("\n"):
            header = line[:16].strip()
            lineData = line[16:]

            if header:
                featureSection = header
                bpRange = lineData
                lastKey = ""
                continue

            if lineData[0] == "/":
                reference = features
                if featureSection != "source":
                    if bpRange not in features:
                        features[bpRange] = {}

                    reference = features[bpRange]

                if "=" not in lineData:
                    if "other" not in reference:
                        reference["other"] = lineData[1:]
                    else:
                        reference["other"] += f", {lineData[1:]}"
                else:
                    key, value = lineData[1:].split("=", 1)
                    lastKey = key
                    if key != "translation":
                        reference[key] = value.strip("\"")
                    
            else:
                if not lastKey: # Multiline base pair range
                    bpRange += lineData
                elif lastKey != "translation":
                    reference[lastKey] += " " + lineData.strip('\"')

        currentData["features_genes"] = str(features)

    elif code == b"SQ":
        currentData["sequence_info"] = data

    else:
        print(f"UNHANDLED code: {code}")
