import requests
import pandas as pd
from pathlib import Path
from lib.tools.progressBar import SteppableProgressBar

def buildURL(keyword: str, perPage: int, page: int = 1) -> str:
    return f"https://collections.museumsvictoria.com.au/api/{keyword}?perpage={perPage}&page={page}&hasimages=yes"

def getAndCheck(propertyDict: dict, property: str, default: any) -> any:
    value = propertyDict.get(property, default)
    if value is None and default is not None:
        return default
    return value

def processEntry(entry: dict) -> list[dict]:
    media = entry["media"]
    taxonomy = entry["taxonomy"]
    speciesID = entry["id"]
    taxonName = taxonomy["taxonName"] if taxonomy is not None else ""

    images = []
    for mediaObject in media:
        if not mediaObject["type"] == "image":
            continue

        for size in ("large", "medium", "small", "thumbnail"):
            image = getAndCheck(mediaObject, size, {})
            if image:
                break
        else:
            continue

        imageFormat = image["uri"].rsplit(".", 1)[-1]
        caption = getAndCheck(mediaObject, "caption", "")
        caption = caption.replace("<em>", "").replace("</em>", "")

        info = {
            "type": "image",
            "format": imageFormat,
            "identifier": image.get("uri", ""),
            "references": f"https://collections.museumsvictoria.com.au/{speciesID}",
            "title": getAndCheck(mediaObject, "alternativeText", ""),
            "description": caption,
            "created": getAndCheck(mediaObject, "dateModiied", ""),
            "creator": getAndCheck(mediaObject, "creators", ""),
            "contributor": "",
            "publisher": "Museums Victoria",
            "audience": "",
            "source": getAndCheck(mediaObject, "sources", ""),
            "license": getAndCheck(mediaObject, "licence", {}).get("name", ""),
            "rightsHolder": getAndCheck(mediaObject, "rightsStatement", ""),
            "datasetID": getAndCheck(mediaObject, "id", ""),
            "taxonName": taxonName,
            "width": getAndCheck(image, "width", 0),
            "height": getAndCheck(image, "height", 0)
        }

        images.append(info)

    return images

def collect():
    baseDir = Path(__file__).parent
    # keywords = ["species", "specimens"]
    entriesPerPage = 100

    headers = {
        "User-Agent": ""
    }

    response = requests.get(buildURL("species", 1), headers=headers)
    totalResults = int(response.headers.get("Total-Results", 0))
    totalCalls = (totalResults / entriesPerPage).__ceil__()

    progressBar = SteppableProgressBar(50, totalCalls)

    entries = []
    for call in range(totalCalls):
        progressBar.update()

        response = requests.get(buildURL("species", entriesPerPage, call+1), headers=headers)
        data = response.json()

        for entry in data:
            entries.extend(processEntry(entry))

    df = pd.DataFrame.from_records(entries)
    df.to_csv(baseDir / "vicMuseumImages.csv", index=False)

if __name__ == "__main__":
    collect()
