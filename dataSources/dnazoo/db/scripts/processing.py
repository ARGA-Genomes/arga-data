import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

def build(outputFilePath: Path) -> None:
    retrieveURL = "https://dnazoo.s3.wasabisys.com/?delimiter=/"
    rawHTML = requests.get(retrieveURL)
    soup = BeautifulSoup(rawHTML.text, "xml")
    
    baseDLURL = "https://dnazoo.s3.wasabisys.com/"

    allData = []
    for idx, species in enumerate(soup.find_all("Prefix")):
        if not species.text:
            continue

        print(" " * 100, end="\r")
        print(f"At species #{idx}: {species.text[:-1]}", end="\r")

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

    df = pd.DataFrame.from_records(allData)
    df.to_csv(outputFilePath, index=False)
