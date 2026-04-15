from pathlib import Path
from lib.secrets import Secrets
import requests
import lib.downloading as dl
from bs4 import BeautifulSoup
import pandas as pd
from lib.processing.scripts import importableScript
import lib.zipping as zp

@importableScript(inputCount=0)
def retrieve(outputDir: Path):
    secrets = Secrets("worms")

    auth = dl.buildAuth(secrets.email, secrets.password)
    response = requests.get("https://www.marinespecies.org/download/", auth=auth)
    soup = BeautifulSoup(response.content, "html.parser")
    dlTable = soup.find("table")
    url = dlTable.find("a")
    downloadURL = url["href"]

    dl.download(downloadURL, outputDir / "worms.zip", verbose=True, auth=auth)

@importableScript()
def combine(outputDir: Path, inputPath: Path):
    extractedFolder = zp.extract(inputPath, outputDir)
    taxonFile = extractedFolder / "taxon.txt"
    speciesProfileFile = extractedFolder / "speciesprofile.txt"
    identifierFile = extractedFolder / "identifier.txt"

    df = pd.read_csv(taxonFile, sep="\t")
    speciesDF = pd.read_csv(speciesProfileFile, sep="\t")
    identDF = pd.read_csv(identifierFile, sep="\t")

    df = df.merge(speciesDF, "outer", "taxonID")
    df = df.merge(identDF, "outer", "taxonID")
    df.to_csv(outputDir / "worms.csv", index=False)
