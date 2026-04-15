from pathlib import Path
from lib.processing.scripts import importableScript
from lib.processing.files import DataFile

@importableScript()
def addName(outputDir: Path, inputFile: DataFile, fileName: str, scientificName, str, **otherFields: dict) -> None:
    df = inputFile.read()
    df["scientific_name"] = scientificName
    for key, value in otherFields.items():
        df[key] = value

    df.to_csv(outputDir / fileName, index=False)
