from pathlib import Path
import pandas as pd

def addName(inPath: Path, outPath: Path, name: str, **otherFields: dict) -> None:
    df = pd.read_csv(inPath)
    df["scientific_name"] = name
    for key, value in otherFields.items():
        df[key] = value

    df.to_csv(outPath, index=False)
