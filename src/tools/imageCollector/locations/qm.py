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
    def getURL(offset: int, count: int) -> str:
        return f"https://collections.qm.qld.gov.au/api/v3/opacobjects?query=collections%3A%2216%22&offset={offset}&limit={count}&direction=asc&hasImages=true&deletedRecords=false&facetedResults=false"

    def getIDUrl(id: str) -> str:
        return f"https://collections.qm.qld.gov.au/api/v3/opacobjects/{id}?view=detail&imageAttributes=true"

    secrets = TomlLoader(dataDir.parent / "secrets.toml")

    session = requests.Session()
    session.headers = {
        "Authorization": f"Basic {secrets.qm.key}",
        "Accept": "application/json"
    }

    retries = Retry(backoff_factor=0.5)
    session.mount("https://", HTTPAdapter(max_retries=retries))

    idsPerCall = 100
    response = session.get(getURL(0, 0))
    data = response.json()

    totalObjects = data["totalObjects"]
    totalCalls = (totalObjects / idsPerCall).__ceil__()

    progress = ProgressBar(totalObjects)
    records = []
    for call in range(totalCalls):
        response = session.get(getURL(call, idsPerCall))
        data = response.json()
        print(data)

        for obj in data["opacObjects"]:
            value = obj["opacObjectId"]
            objResponse = session.get(getIDUrl(value))
            try:
                objData = objResponse.json()
            except:
                print(objResponse.content, objResponse.status_code)
                return

            records.extend(parseObject(value, objData))
            progress.update()

    df = pd.DataFrame.from_records(records)
    df.to_csv(dataDir / "qm.csv", index=False)
