import pandas as pd
from pathlib import Path
from lib.processing.scripts import importableScript
from lib.processing.files import DataFile

@importableScript()
def clean(outputDir: Path, inputFile: DataFile) -> None:
    # Load to csv and resave to remove quotation marks around data
    df = inputFile.read()
    df.to_csv(outputDir / inputFile.path.name, index=False)
