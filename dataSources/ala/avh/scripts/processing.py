import pandas as pd
import requests
from pathlib import Path


def build(outputFilePath: Path) -> None:
    baseURL = "https://biocache-ws.ala.org.au/ws/occurrences/search?q=*%3A*&disableAllQualityFilters=true&qualityProfile=AVH&fq=type_status%3A*&fq=country%3A%22Australia%22&qc=data_hub_uid%3Adh9"
    firstCall = baseURL + "&pageSize=0"
    readSize = 1000

    rawData = requests.get(firstCall)
    jsData = rawData.json()

    records = jsData["totalRecords"]
    totalCalls = (records / readSize).__ceil__()

    occurrences = []
    for call in range(totalCalls):
        callURL = baseURL + f"&pageSize={readSize}" + f"&startIndex={call*readSize}"
        print(f"At call: {call+1} / {totalCalls}", end="\r")
        rawData = requests.get(callURL)
        jsData = rawData.json()
        occurrences.extend(jsData["occurrences"])

    df = pd.DataFrame.from_records(occurrences)
    df.to_csv(outputFilePath, index=False)
