from pathlib import Path
import pandas as pd

def process(folderPath: Path, outputPath: Path) -> None:

    def readCSV(fileName: str) -> pd.DataFrame:
        return pd.read_csv(folderPath / fileName, sep="\t", on_bad_lines="skip", low_memory=False)
    
    df = readCSV("Taxon.tsv")

    speciesProfile = readCSV("SpeciesProfile.tsv")
    df = df.merge(speciesProfile, "left", "dwc:taxonID")

    vernacularNames = readCSV("VernacularName.tsv")
    records = {}
    for _, row in vernacularNames.iterrows():
        taxID = row["dwc:taxonID"]
        if taxID not in records:
            records[taxID] = {}

        language = row["dcterms:language"]
        if language not in records[taxID]:
            records[taxID][language] = []

        records[taxID][language].append(row["dwc:vernacularName"])

    vernacular = pd.DataFrame.from_dict(records, orient="index")
    df = df.merge(vernacular, "left", left_on="dwc:taxonID", right_on=vernacular.index)
    df.to_csv(outputPath, index=False)
