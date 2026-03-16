import pandas as pd
from pathlib import Path

def clean(filePath: Path, outputFilePath: Path) -> None:
    # Load to csv and resave to remove quotation marks around data
    df = pd.read_csv(filePath)
    df.to_csv(outputFilePath, index=False)
