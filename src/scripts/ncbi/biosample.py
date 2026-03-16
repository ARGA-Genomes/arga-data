import pandas as pd
from pathlib import Path

def splitLatLong(inputPath: Path, outputPath: Path) -> None:
    df = pd.read_csv(inputPath)
    df[["decimalLatitude", "decimalLongitude"]] = df["ncbi_lat long"].str.split(" ", expand=True)
    df = df.drop("ncbi_lat long", axis=1)
    df.to_csv(outputPath, index=False)
