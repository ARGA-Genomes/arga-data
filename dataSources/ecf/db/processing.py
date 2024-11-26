from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lib.tools.progressBar import SteppableProgressBar

def getMetadata(filePath: Path, outputFile: Path):
    outputFolder = outputFile.parent / "metadata"
    outputFolder.mkdir(exist_ok=True)

    df = pd.read_csv(filePath, sep="\t")
    session = requests.Session()

    ids = df[df["URL"].notna()]
    progress = SteppableProgressBar(50, len(ids), "Scraping")

    for _, row in ids.iterrows():
        url = row["URL"]
        id = row["ID"]

        outputFile = outputFolder / f"{id}.txt"
        if not outputFile.exists():
            response = session.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            content = soup.find("p", {"class": "result"})

            with open(outputFile, "w", encoding="utf-8") as fp:
                fp.write(content.text)

        progress.update()
