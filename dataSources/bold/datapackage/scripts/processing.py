import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from pathlib import Path
import logging
from lib.secrets import secrets
import lib.downloading as dl

def retrieve(outputFilePath: Path):
    baseURL = "https://bench.boldsystems.org/index.php"
    session = requests.Session()

    logging.info("Collecting latest date for bold package")
    response = session.get(f"{baseURL}/datapackages/Latest")
    soup = BeautifulSoup(response.content, "html.parser")

    for header in soup.find_all("h2"):
        text = header.text
        if text.startswith("BOLD DNA Barcode Reference Library"):
            break
    else:
        logging.error("No header found for retrieving date")
        exit()

    rawDate = text.rsplit(" ", 1)[-1]
    day, month, year = rawDate.split("-")
    date = f"{day}-{month.capitalize()}-{year}"
    logging.info(f"Latest datapackage date: {date}")

    logging.info("Retrieving uid for download")
    datapackageURL = f"{baseURL}/API_Datapackage?id=BOLD_Public.{date}"
    response = session.get(datapackageURL)
    uid = response.text.strip('"')
    downloadURL = f"{datapackageURL}&uid={uid}"

    dl.download(downloadURL, outputFilePath, verbose=True, auth=dl.buildAuth(secrets.bold.username, secrets.bold.password))

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