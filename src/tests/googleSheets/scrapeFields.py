import pandas as pd
import json
from lib.settings import globalSettings as gs
import numpy as np

events = ["Event 1: Collection", "Event 2: Sample Accession", "Event 3: Subampling", "Event 4: Preparation", "Event 5: Extraction", "Event 6: Sequencing", "Event 7: Assembly", "Event 8: Annotation", "Event 9: Data Accession / Release"]

def getSheet(sheetID):
    docID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"
    url = f"https://docs.google.com/spreadsheets/d/{docID}/export?format=csv&gid={sheetID}"
    return pd.read_csv(url, keep_default_na=False)

def getMappings(df):
    df.to_csv("before.csv", index=False)
    relevantColumns = ["Field Name"] + [col for col in df.columns if col.startswith("Event")]
    # relevantColumns = ["Field Name"] + events
    df = df[relevantColumns]
    # print(df.tail())
    
    mappings = {}
    for idx, row in df.iterrows():
        options = []
        for colName, value in row.items():
            if colName == "Field Name":
                continue

            if not isinstance(value, str): # Ignore float/int
                continue

            if value in ("0", "1", "nan", "NaN", np.nan, np.NaN, np.NAN) or value.startswith("ARGA"):
                continue
            
            elements = [elem.strip() for elem in value.split(",")]
            options.extend([elem for elem in elements if elem not in options])
        
        mappings[row["Field Name"]] = options

    return mappings

def getSheetMappings(sheetID):
    return getMappings(getSheet(sheetID))

def main():
    sheetIDs = {
        "ncbi-refseq": 1264640954,
        "42bp-genomeArk": 84855374,
        "anemone-db": 286004534,
        "bold-tsv": 1099164815
    }

    for key, value in sheetIDs.items():
        location, db = key.split("-")
        print(f"Creating map for {key}")
        mappings = getSheetMappings(value)
        mapFile = gs.folderPaths.mapping / f"{location}.json"

        if not mapFile.exists():
            with open(mapFile, "w") as fp:
                json.dump(mappings, fp, indent=4)
            continue

        with open(mapFile) as fp:
            columnMap = json.load(fp)

        for keyword, names in mappings.items():
            if keyword not in columnMap:
                columnMap[keyword] = names
            else:
                columnMap[keyword].extend(names)

        with open(mapFile, "w") as fp:
            json.dump(columnMap, fp, indent=4)


if __name__ == "__main__":
    main()
