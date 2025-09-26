import requests
from lib.progressBar import ProgressBar
import pandas as pd
from lib.tomlFiles import TomlLoader
from requests.adapters import HTTPAdapter, Retry

def parseObject(id: str, data: dict) -> list[dict]:
    identificationData = {"id": id}
    identifiers = data["opacObjectFieldSets"]
    for property in identifiers:
        identKey = property["identifier"]
        identValue = ", ".join(field["value"] for field in property["opacObjectFields"])
        
        identificationData[identKey] = identValue

    records = []

    images = data["imagesCollection"]["images"]
    for image in images:
        imageData = image["imageDerivatives"][0] # First image derivative is largest
        records.append(identificationData | imageData)

    return records

def run(dataDir):
    idsPerCall = 100
    baseURL = "https://collections.qm.qld.gov.au/api/v3/opacobjects"

    query = {
        "query": "collections:\"16\"",
        "direction": "asc",
        "hasImages": True,
        "deletedRecords": False,
        "facetedResults": False,
    }

    idParams = {
        "view": "detail",
        "imageAttributes": True
    }

    secrets = TomlLoader(dataDir.parent / "secrets.toml")

    session = requests.Session()
    session.headers = {
        "Authorization": f"Basic {secrets.qm.key}",
        "Accept": "application/json"
    }

    retries = Retry(backoff_factor=1, status_forcelist=[403])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    response = session.get(baseURL, params=query | {"offset": 0, "limit": 0})
    data = response.json()

    totalObjects = data["totalObjects"]
    totalCalls = (totalObjects / idsPerCall).__ceil__()

    progress = ProgressBar(totalObjects)
    records = []
    for call in range(totalCalls):
        response = session.get(baseURL, params=query | {"offset": call, "limit": idsPerCall})
        data = response.json()

        for obj in data["opacObjects"]:
            opacID = obj["opacObjectId"]

            response = session.get(f"{baseURL}/{opacID}", params=idParams)
            data = response.json()
            
            records.extend(parseObject(opacID, data))
            progress.update()

    df = pd.DataFrame.from_records(records)
    df.to_csv(dataDir / "qm.csv", index=False)
