from pathlib import Path
import pandas as pd
from lib.processing.scripts import importableScript
import lib.zipping as zp

@importableScript()
def unpack(outputDir: Path, inputPath: Path):
    extractedFolder = zp.extract(inputPath, outputDir)

    def loadFile(fileName: str) -> pd.DataFrame:
        return pd.read_csv(extractedFolder / fileName, sep="|", low_memory=False)
    
    taxonomy = loadFile("wcvp_taxon.csv")
    names = loadFile("wcvp_replacementNames.csv")

    taxonomy = taxonomy.merge(names, "left", "taxonid")
    taxonomy["nomenclatural_code"] = "ICZN"

    taxonomy.to_csv(outputDir / "powo.csv", index=False)
