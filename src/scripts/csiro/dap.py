import math
import requests
import pandas as pd

def getPortalData(outputFilePath: Path) -> None:
    baseURL = "https://data.csiro.au/dap/ws/v2/collections"
    entriesPerPage = 100

    # Get page 1 first, then iterate over remaining pages
    response = requests.get(f"{baseURL}?rpp={entriesPerPage}&p={1}")
    data = response.json()
    records = data["dataCollections"]

    totalResults = data["totalResults"]
    totalCalls = math.ceil(int(totalResults) / entriesPerPage)

    for call in range(1, totalCalls):
        response = requests.get(f"{baseURL}?rpp={entriesPerPage}&p={call+1}")
        data = response.json()
        records.extend(data["dataCollections"])

    for record in records:
        record |= record.pop("spatialParameters", {})

    df = pd.DataFrame.from_records(records)
    # idDf = df["id"].apply(lambda x: dict(x)).apply(pd.Series)
    # df.drop("id", axis=1, inplace=True)
    # df = pd.concat([idDf, df], axis=1)
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    df.to_csv(outputFilePath, index=False)
