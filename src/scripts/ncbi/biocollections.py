from pathlib import Path
import pandas as pd
from lib.processing.scripts import importableScript

@importableScript(inputCount=2)
def compile(outputDir: Path, collectionCodesPath: Path, institutionCodesPath: Path) -> None:
    collectionCodes = pd.read_csv(collectionCodesPath, sep="\t|\t", engine="python", on_bad_lines="skip", dtype=object)
    institutionCodes = pd.read_csv(institutionCodesPath, sep="\t|\t", engine="python", on_bad_lines="skip", dtype=object)
    df = pd.merge(collectionCodes, institutionCodes, "left", on="inst_id")
    df = df.dropna(how="all", axis=1)
    df.to_csv(outputDir / "biocollections.csv", index=False)
