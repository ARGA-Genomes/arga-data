from pathlib import Path
import lib.secrets as scr
import requests
import lib.downloading as dl
from bs4 import BeautifulSoup
import pandas as pd

def retrieve(outputFilePath: Path):
    secrets = scr.load()

    auth = dl.buildAuth(secrets.general.email, secrets.worms.password)
    response = requests.get("https://www.marinespecies.org/download/", auth=auth)
    soup = BeautifulSoup(response.content, "html.parser")
    dlTable = soup.find("table")
    url = dlTable.find("a")
    downloadURL = url["href"]

    dl.download(downloadURL, outputFilePath, verbose=True, auth=auth)

def combine(folderPath: Path, outputFilePath: Path):
    taxonFile = folderPath / "taxon.txt"
    speciesProfileFile = folderPath / "speciesprofile.txt"
    identifierFile = folderPath / "identifier.txt"

    df = pd.read_csv(taxonFile, sep="\t")
    speciesDF = pd.read_csv(speciesProfileFile, sep="\t")
    identDF = pd.read_csv(identifierFile, sep="\t")

    df = df.merge(speciesDF, "outer", "taxonID")
    df = df.merge(identDF, "outer", "taxonID")
    df.to_csv(outputFilePath, index=False)
