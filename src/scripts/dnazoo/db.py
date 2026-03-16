import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from lib.progressBar import ProgressBar

def build(outputFilePath: Path) -> None:
    retrieveURL = "https://dnazoo.s3.wasabisys.com/?delimiter=/"
    baseDLURL = "https://dnazoo.s3.wasabisys.com/"

    rawHTML = requests.get(retrieveURL)
    soup = BeautifulSoup(rawHTML.text, "xml")
    allSpecies = soup.find_all("Prefix")

    progress = ProgressBar(len(allSpecies))

    allData = []
    for species in allSpecies:
        if not species.text:
            continue

        dataURL = baseDLURL + species.text + "README.json"
        rawData = requests.get(dataURL)
        if rawData.status_code != requests.codes.ok:
            continue # No JSON for this species

        try:
            data = rawData.json()
        except requests.exceptions.JSONDecodeError:
            continue # Error with json decoding, skip

        flatData = {}
        for key in list(data.keys()):
            value = data.pop(key)

            if isinstance(value, dict):
                flatData |= {f"{key}_{k}": v for k, v in value.items()}
            else:
                flatData |= {key: value}

        allData.append(flatData)
        progress.update()

    df = pd.DataFrame.from_records(allData)
    df.to_csv(outputFilePath, index=False)
