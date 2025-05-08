import pandas as pd
import requests
from pathlib import Path
from lib.progressBar import SteppableProgressBar

def build(outputFilePath: Path) -> None:
    baseURL = "https://biocache-ws.ala.org.au/ws/occurrences/search?"
    fields = {
        "q": "basis_of_record%3AOCCURRENCE%20PRESERVED_SPECIMEN%20MATERIAL_SAMPLE%20LIVING_SPECIMEN%20MATERIAL_CITATION",
        "qualityProfile": "ALA",
        "disableQualityFilter": [
                "spatially-suspect",
                "location",
                "location-uncertainty",
                "outliers",
                "scientific-name",
                "record-type",
                "user-assertions",
                "dates-post-1700",
                "degree-of-establishment",
                "verification-status"
            ],
        "qc": "-_nest_parent_%3A*"
    }

    flatFields = []
    for fieldName, value in fields.items():
        if isinstance(value, str):
            flatFields.append(f"{fieldName}={value}")
            continue

        for item in value:
            flatFields.append(f"{fieldName}={item}")

    fullURL = f"{baseURL}{'&'.join(flatFields)}"

    firstCall = fullURL + "&pageSize=0"
    readSize = 1000

    response = requests.get(firstCall)
    data = response.json()

    records = data["totalRecords"]
    totalCalls = (records / readSize).__ceil__()

    progress = SteppableProgressBar(totalCalls)
    records = []
    for call in range(totalCalls):
        url = f"{fullURL}&pageSize={readSize}&startIndex={call*readSize}"
        response = requests.get(url)
        data = response.json()
        records.extend(data["occurrences"])
        progress.update()

    df = pd.DataFrame.from_records(records)
    df.to_csv(outputFilePath, index=False)
