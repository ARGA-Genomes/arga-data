import requests
from pathlib import Path
import pandas as pd
import json
import lib.dataframes as dff

def getUrl(entryCount: int, page: int) -> str:
    baseURL = "https://api.algaebase.org/v1.3/"
    endpoint = "species"
    query = "taxonomicstatus=C"
    return f"{baseURL}{endpoint}?{query}&count={entryCount}&offset={page * entryCount}"

def build(outputFile: Path, apiKeyPath: Path) -> None:
    with open(apiKeyPath) as fp:
        apiKey = fp.read()

    headers = {
        "abapikey": apiKey
    }

    entriesPerCall = 1000
    response = requests.get(getUrl(entriesPerCall, 0), headers=headers)
    data = response.json()
    
    records = data["result"]
    totalCalls = data["_pagination"]["_total_number_of_pages"]

    for call in range(1, totalCalls):
        print(f"At call: {call} / {totalCalls - 1}", end="\r")
        response = requests.get(getUrl(entriesPerCall, call), headers=headers)

        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"\nError with call {call}")
            continue

        records.extend(data["result"])

    df = pd.DataFrame.from_records(records)
    df = dff.removeSpaces(df)
    df.to_csv(outputFile, index=False)