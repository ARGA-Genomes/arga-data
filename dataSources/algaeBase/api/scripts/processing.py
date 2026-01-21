import requests
from pathlib import Path
import pandas as pd
import json
import lib.dataframes as dff
from lib.progressBar import ProgressBar
import lib.secrets as scr
import logging

def build(outputFile: Path) -> None:
    secrets = scr.load()
    entriesPerCall = 1000
    
    headers = {
        "abapikey": secrets.algaebase.key
    }

    def getApiPage(page: int) -> dict:
        url = f"https://api.algaebase.org/v1.3/species?taxonomicstatus=C&count={entriesPerCall}&offset={page * entriesPerCall}"
        response = requests.get(url, headers=headers)
        try:
            return response.json()
        except json.JSONDecodeError:
            logging.error(f"Unable to retrieve data on page: {page}")
            return {}

    data = getApiPage(0)
    records: list = data["result"]
    totalCalls = data["_pagination"]["_total_number_of_pages"]

    progress = ProgressBar(totalCalls)
    for call in range(1, totalCalls):
        data = getApiPage(call)
        records.extend(data.get("result", []))
        progress.update()

    df = pd.DataFrame.from_records(records)
    df = dff.removeSpaces(df)
    df.to_csv(outputFile, index=False)
