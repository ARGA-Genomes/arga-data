import requests
from lib.tools.progressBar import SteppableProgressBar
import pandas as pd

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

def collect():
    def getURL(offset: int, count: int) -> str:
        return f"https://collections.qm.qld.gov.au/api/v3/opacobjects?query=collections%3A%2216%22&offset={offset}&limit={count}&direction=asc&hasImages=true&deletedRecords=false&facetedResults=false"

    def getIDUrl(id: str) -> str:
        return f"https://collections.qm.qld.gov.au/api/v3/opacobjects/{id}?view=detail&imageAttributes=true"

    headers = {
        "Authorization": "Basic NjphMzZkZGUwN2IyNWE0NDExYzIwNThmNjI2Y2QxMjdkMDRhZmEyY2QzNmM4ZTBjYWRiMTkxMDY4NDBhMjVlMmM=",
        "Accept": "application/json"
    }

    session = requests.Session()
    session.headers = headers

    idsPerCall = 100
    response = session.get(getURL(0, 0))
    data = response.json()

    totalObjects = data["totalObjects"]
    totalCalls = (totalObjects / idsPerCall).__ceil__()

    progress = SteppableProgressBar(50, totalObjects)
    records = []

    for call in range(totalCalls):
        response = session.get(getURL(call, idsPerCall))
        data = response.json()

        for obj in data["opacObjects"]:
            value = obj["opacObjectId"]
            objResponse = session.get(getIDUrl(value))
            objData = objResponse.json()

            records.extend(parseObject(value, objData))
            progress.update()

    df = pd.DataFrame.from_records(records)
    df.to_csv("qm.csv", index=False)

if __name__ == "__main__":
    collect()
