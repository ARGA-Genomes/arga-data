import requests
import pandas as pd
from pathlib import Path
import lib.dataframes as dff

def getPortalData(outputFilePath: Path) -> None:
    baseURL = "https://data.csiro.au/dap/ws/v2/collections"
    entriesPerPage = 100

    currentPage = 1
    allRecords = []
    while True:
        print(f"Collecting page: {currentPage}", end="\r")
        response = requests.get(f"{baseURL}?rpp={entriesPerPage}&p={currentPage}", headers={"Content-Type": "application/json"})
        data = response.json()
        records = data.get("dataCollections", [])
        allRecords.extend(records)
        currentPage += 1

        if len(records) < entriesPerPage:
            break

    df = pd.DataFrame.from_records(allRecords)
    df = dff.removeSpaces(df)
    df.to_csv(outputFilePath, index=False)
