import requests
from bs4 import BeautifulSoup

import numpy as np
import pandas as pd
from pathlib import Path
from lib.processing.scripts import importableScript
from lib.processing.files import DataFile
import lib.zipping as zp
import lib.common as cmn

@importableScript(inputCount=0)
def retrieve(outputDir: Path):
    # datapackageURL = "https://www.boldsystems.org/index.php/datapackages"
    # request = requests.get(datapackageURL)

    # tables = pd.read_html(request.text)
    # recentData = tables[0]
    # mostRecent = recentData["Snapshot Date"][0]
    mostRecent = "28-APR-2023"

@importableScript()
def cleanUp(outputDir: Path, inputFile: DataFile) -> None:
    extractedFile = zp.extract(inputFile.path, outputDir)

    tsvFile = [file for file in extractedFile.iterdir() if file.suffix == ".tsv"][0] # Should only be one file
    tsvFile.rename(outputDir / "datapackage.tsv")
    cmn.clearFolder(extractedFile, delete=True)

def augment(df: pd.DataFrame) -> pd.DataFrame:
    clusterPrefix = "http://www.boldsystems.org/index.php/Public_BarcodeCluster?clusteruri="
    df['species'] = df['species'].fillna("sp. {" + df['bold_bin_uri'].astype(str) + "}")
    df['bold_bin_uri'] = np.where(df['bold_bin_uri'].notna(), clusterPrefix + df['bold_bin_uri'], df['bold_bin_uri'])

    return df