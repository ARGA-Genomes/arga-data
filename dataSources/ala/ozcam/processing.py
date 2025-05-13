import pandas as pd
import requests
from pathlib import Path
from lib.progressBar import SteppableProgressBar
from lib.bigFileWriter import BigFileWriter

def build(outputFilePath: Path) -> None:
    baseURL = "https://biocache-ws.ala.org.au/occurrences/occurrences/offline/download?"
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
    recordsPerPage = 10000

    response = requests.get(firstCall)
    print(response.content)
    data = response.json()

    totalRecords = data["totalRecords"]
    totalCalls = (totalRecords / recordsPerPage).__ceil__()

    writer = BigFileWriter(outputFilePath)
    # progress = SteppableProgressBar(totalCalls)

    records = []
    for call in range(totalCalls):
        if call == 10:
            return
            
        response = requests.get(f"{fullURL}&pageSize={recordsPerPage}&start={call*recordsPerPage}")
        data = response.json()
        print(len(data["occurrences"]))
        continue
        records.extend(data.get("occurrences", []))

        if len(records) >= 1000000:
            writer.writeDF(pd.DataFrame.from_records(records))
            records.clear()

        progress.update()

    writer.writeDF(pd.DataFrame.from_records(records))
    writer.oneFile()
