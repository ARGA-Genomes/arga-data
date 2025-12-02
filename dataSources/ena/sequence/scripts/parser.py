from pathlib import Path
import pandas as pd
from lib.progressBar import ProgressBar
import logging

def parseFile(filePath: Path) -> pd.DataFrame | None:
    logging.info(f"Parsing flat file: {filePath}")

    with open(filePath) as fp:
        sections = fp.read().split("\n//\n")

    # Iterate through sections of file
    progress = ProgressBar(len(sections))
    records = []
    for section in sections:
        records.append(parseSection(section))
        progress.update()

    return pd.DataFrame.from_records(records)

def parseSection(sectionData: str) -> dict:
    data = {}
    code = ""
    codeRepeat = 0

    dataClass = {
        "EST": "expressed sequence tag",
        "GSS": "genome survey sequence",
        "STS": "sequence tagged site",
        "CON": "constructed sequence",
        "TSA": "transcriptome shotgun assembly",
        "HTC": "high throughput assembled transcriptomic sequence",
        "HTG": "high throughput assembled genomic sequence",
        "STD": "standard targeted annotated assembled sequence",
        "PAT": "sequence associated with a patent process",
        "WGS": "whole genome shotgun contig level assembly"
    }

    taxonClass = {
        "FUN": "Fungi",
        "ENV": "Environmental",
        "HUM": "Human",
        "INV": "Invertebrate",
        "MAM": "Mammalia",
        "MUS": "MusMusculus",
        "PHG": "Phage",
        "PLN": "Plants",
        "PRO": "Pro",
        "ROD": "Rodent",
        "SYN": "Synthetic",
        "UNC": "Unclassified",
        "VRL": "Virus",
        "VRT": "Vertebrate",
        "XXX": "Unknown"
    }

    for line in sectionData.split("\n"):
        if line[:2] == code:
            codeRepeat += 1
        else:
            code = line[:2]
            codeRepeat = 0

        if code == "XX" or code == "  " or not code:
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
            data["accession"] = lineData.rstrip(";")

        elif code == "PR":
            key, value = lineData[:-1].split(":", 1)
            data[key.lower()] = value

        elif code == "DT":
            if codeRepeat == 0:
                data["original_date"] = lineData
            elif codeRepeat == 1:
                data["date"] = lineData

        elif code == "DE":
            if codeRepeat == 0:
                data["description"] = lineData
            else:
                data["description"] += f" {lineData}"

        elif code == "KW":
            continue

        elif code == "OS":
            data["scientific_name"] = lineData

        elif code == "OC":
            if codeRepeat == 0:
                data["lineage"] = lineData
            else:
                data["lineage"] += f" {lineData}"

        elif code == "OG":
            data["sample"] = lineData

        elif code == "RN":
            if "references" not in data:
                data["references"] = []
            
            data["references"].append({})

        elif code == "RP":
            data["references"][-1]["base_range"] = lineData

        elif code == "RC":
            data["references"][-1]["comment"] = lineData

        elif code == "RX":
            link, value = lineData[:-1].split("; ", 1)
            data["references"][-1][link.lower()] = value

        elif code == "RA":
            if codeRepeat == 0:
                data["references"][-1]["authors"] = lineData.strip(";")
            else:
                data["references"][-1]["authors"] += f" {lineData.strip(';')}"

        elif code == "RT":
            lineData = lineData.strip("\";")
            if codeRepeat == 0:
                data["references"][-1]["title"] = lineData
            else:
                data["references"][-1]["title"] += f" {lineData}"

        elif code == "RL":
            data["references"][-1]["literature"] = lineData[:-1]

        elif code == "RG":
            data["references"][-1]["group"] = lineData

        elif code == "DR":
            if "data_reference" not in data:
                data["data_reference"] = {}

            try:
                key, value = lineData[:-1].split("; ", 1)
            except:
                print(lineData)
                return
            
            if key not in data["data_reference"]:
                data["data_reference"][key] = value
            else:
                data["data_reference"][key] += f", {value}"

        elif code == "CC":
            continue

        elif code == "FH":
            featureSection = ""
            bpRange = ""
            lastKey = ""

            data["features_genes"] = {}

        elif code == "FT":
            header = lineData[:16].strip()
            lineData = lineData[16:]

            if header:
                featureSection = header
                bpRange = lineData
                lastKey = ""
                continue

            if lineData[0] == "/":
                reference = data
                if featureSection != "source":
                    if bpRange not in data["features_genes"]:
                        data["features_genes"][bpRange] = {}

                    reference = data["features_genes"][bpRange]

                if "=" not in line:
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

        elif code == "SQ":
            data["sequence_info"] = lineData

        elif code == "CO":
            continue

        else:
            print(f"UNHANDLED CODE: {code}")

    return data
