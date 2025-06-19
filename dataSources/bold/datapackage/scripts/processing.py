import requests
from bs4 import BeautifulSoup

import numpy as np
import pandas as pd
from pathlib import Path

def retrieve(outputFilePath: Path):
    # datapackageURL = "https://www.boldsystems.org/index.php/datapackages"
    # request = requests.get(datapackageURL)

    # tables = pd.read_html(request.text)
    # recentData = tables[0]
    # mostRecent = recentData["Snapshot Date"][0]
    mostRecent = "28-APR-2023"

def cleanUp(folderPath: Path, outputFilePath: Path) -> None:
    for file in folderPath.iterdir():
        if file.suffix == ".tsv":
            file.rename(outputFilePath)
            continue

        file.unlink()

    folderPath.rmdir() # Cleanup remaining folder

def augment(df: pd.DataFrame) -> pd.DataFrame:
    clusterPrefix = "http://www.boldsystems.org/index.php/Public_BarcodeCluster?clusteruri="
    df['species'] = df['species'].fillna("sp. {" + df['bold_bin_uri'].astype(str) + "}")
    df['bold_bin_uri'] = np.where(df['bold_bin_uri'].notna(), clusterPrefix + df['bold_bin_uri'], df['bold_bin_uri'])

    return df