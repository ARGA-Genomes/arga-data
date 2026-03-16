from pathlib import Path
import pandas as pd

def combine(folderPath: Path, outputFilePath: Path):
    
    def loadFile(fileName: str) -> pd.DataFrame:
        return pd.read_csv(folderPath / fileName, sep="|", low_memory=False)
    
    taxonomy = loadFile("wcvp_taxon.csv")
    names = loadFile("wcvp_replacementNames.csv")

    taxonomy = taxonomy.merge(names, "left", "taxonid")
    taxonomy["nomenclatural_code"] = "ICZN"

    taxonomy.to_csv(outputFilePath, index=False)
