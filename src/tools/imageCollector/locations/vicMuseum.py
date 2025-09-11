import requests
import pandas as pd
from pathlib import Path
from lib.progressBar import ProgressBar

def processEntry(entry: dict) -> list[dict]:
    media = entry["media"]
    taxonomy = entry["taxonomy"]
    speciesID = entry["id"]
    taxonName = taxonomy["taxonName"] if taxonomy is not None else ""

    images = []
    for mediaObject in media:

        def getAndReplaceNone(obj: dict, attr: str, default: any) -> any:
            value = obj.get(attr, default)
            return value if value is not None else default

        if not mediaObject["type"] == "image":
            continue

        for size in ("large", "medium", "small", "thumbnail"):
            image = getAndReplaceNone(mediaObject, size, {})
            if image:
                break
        else:
            continue

        imageFormat = image["uri"].rsplit(".", 1)[-1]
        caption = getAndReplaceNone(mediaObject, "caption", "")
        caption = caption.replace("<em>", "").replace("</em>", "")

        info = {
            "type": "image",
            "format": imageFormat,
            "identifier": image.get("uri", ""),
            "references": f"https://collections.museumsvictoria.com.au/{speciesID}",
            "title": getAndReplaceNone(mediaObject, "alternativeText", ""),
            "description": caption,
            "created": getAndReplaceNone(mediaObject, "dateModiied", ""),
            "creator": getAndReplaceNone(mediaObject, "creators", ""),
            "contributor": "",
            "publisher": "Museums Victoria",
            "audience": "",
            "source": getAndReplaceNone(mediaObject, "sources", ""),
            "license": getAndReplaceNone(mediaObject, "licence", {}).get("name", ""),
            "rightsHolder": getAndReplaceNone(mediaObject, "rightsStatement", ""),
            "datasetID": getAndReplaceNone(mediaObject, "id", ""),
            "taxonName": taxonName,
            "width": getAndReplaceNone(image, "width", 0),
            "height": getAndReplaceNone(image, "height", 0)
        }

        images.append(info)

    return images

def run():
    baseDir = Path(__file__).parents[1]
    dataDir = baseDir / "data"

    baseURL = "https://collections.museumsvictoria.com.au/api/"
    # keywords = ["species", "specimens"]
    entriesPerPage = 100

    headers = {
        "User-Agent": ""
    }

    def getParams(page: int, perPage: int) -> dict:
        return {"page": page, "perPage": perPage, "hasimages": "yes"}

    session = requests.Session()

    response = session.head(baseURL + "species", headers=headers, params=getParams(1, 1))
    totalResults = int(response.headers.get("Total-Results", 0))
    totalCalls = (totalResults / entriesPerPage).__ceil__()
    progress = ProgressBar(totalResults)

    entries = []
    for call in range(totalCalls):
        response = session.get(baseURL + "species", headers=headers, params=getParams(call, entriesPerPage))
        data = response.json()

        for entry in data:
            entries.extend(processEntry(entry))
            progress.update()

    df = pd.DataFrame.from_records(entries)
    df.to_csv(dataDir / "vicMuseum.csv", index=False)
