from pathlib import Path
import requests
import pandas as pd
from lib.processing.scripts import importableScript

@importableScript(inputCount=0)
def build(outputDir: Path, location: str) -> None:
    baseURL = "https://appliedgenomics.csiro.au/"
    htmlData = requests.get(baseURL + location)
    df = pd.read_html(htmlData.text)[0] # Returned list is 1 long, only 1 table on page
    df.dropna(axis=1, inplace=True)
    df.to_csv(outputDir / f"{location}.csv", index=False)
