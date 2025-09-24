import requests
from bs4 import BeautifulSoup
from pathlib import Path
from lib.progressBar import ProgressBar
import pandas as pd

def run(dataDir: Path):
    baseURL = "https://museum.wa.gov.au"
    endpoint = "/online-collections/waminals"
    page = 0
    session = requests.Session()

    species = {}
    while True:
        print(f"Collecting page: {page}", end="\r")
        response = session.get(baseURL + endpoint, params={"page": page})
        soup = BeautifulSoup(response.content, "html.parser")
        teasers = soup.find_all("div", {"class": "teasernode"})

        if not teasers:
            break

        for teaser in teasers:
            href = teaser.find("a")["href"]

            italicText = teaser.find("i")
            if italicText is None:
                species[href] = ("", teaser.text.strip())
                continue

            scientificNamePos = teaser.text.find(italicText.text)
            species[href] = (teaser.text[:scientificNamePos].strip(), teaser.text[scientificNamePos:].strip())

        page += 1

    print("Collecting images for each species")
    imageData = []
    progress = ProgressBar(len(species))
    for href, (commonName, taxonName) in species.items():
        response = session.get(baseURL + href)
        soup = BeautifulSoup(response.content, "html.parser")

        images = soup.find("div", {"class": "royalslider royalSlider rsDefault"})
        for div in images.find_all("div", recursive=False):
            imageInfo = div.find("a")
            imageURL = imageInfo["href"]

            response = requests.head(imageURL)
            dataType, dataFormat = response.headers["Content-Type"].split("/")

            imageData.append({
                "type": dataType,
                "format": dataFormat,
                "identifier": imageURL,
                "references": Path(imageInfo["href"]).stem,
                "title": imageInfo["title"],
                "created": response.headers["Last-Modified"],
                "publisher": "WA Museum",
                "source": "https://museum.wa.gov.au/",
                "taxonName": taxonName,
                "commonName" : commonName,
                "size": response.headers["Content-Length"]
            })

        progress.update()

    df = pd.DataFrame.from_records(imageData)
    df.to_csv(dataDir / "waminals.csv", index=False)
