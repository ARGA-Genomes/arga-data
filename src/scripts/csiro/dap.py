import math
import requests
import pandas as pd
from lib.processing.scripts import importableScript
from pathlib import Path

@importableScript(inputCount=0)
def getPortalData(outputDir: Path) -> None:
    baseURL = "https://data.csiro.au/dap/ws/v2/collections"
    entriesPerPage = 100

    session = requests.Session()
    def getRecords(page: int) -> list[dict]:
        response = session.get(f"{baseURL}?rpp={entriesPerPage}&p={page}")
        data = response.json()
        return data.get("dataCollections", [])

    totalRecords = []
    currentPage = 1
    while True:
        print(f"At page: {currentPage}", end="\r")
        records = getRecords(currentPage)
        totalRecords.extend(records)
        if len(records) < entriesPerPage:
            break

        currentPage += 1

    df = pd.DataFrame.from_records(totalRecords)
    # idDf = df["id"].apply(lambda x: dict(x)).apply(pd.Series)
    # df.drop("id", axis=1, inplace=True)
    # df = pd.concat([idDf, df], axis=1)
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    df.to_csv(outputDir / "dap.csv", index=False)
