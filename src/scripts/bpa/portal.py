from pathlib import Path
import requests
import pandas as pd
from lib.progressBar import ProgressBar

def build(outputFilePath: Path, entriesPerPage: int) -> None:
    url = "https://data.bioplatforms.com/api/3/action/package_search?q=*:*&rows="

    initialRequest = requests.get(f"{url}{0}")
    initialJson = initialRequest.json()

    summary = initialJson.get("result", {})
    totalEntries = summary.get("count", 0)

    if totalEntries == 0:
        print("No entries found, quitting...")
        return
    
    numberOfCalls = (totalEntries / entriesPerPage).__ceil__()
    progress = ProgressBar(numberOfCalls)

    entries = []
    for call in range(numberOfCalls):
        response = requests.get(f"{url}{entriesPerPage}&start={call*entriesPerPage}")
        data: dict = response.json()

        results = data.get("result", {}).get("results", [])
        entries.extend(results)

        progress.update()

    df = pd.DataFrame.from_records(entries)
    df["bpa_url"] = "https://data.bioplatforms.com/" + df["type"] + "/" + df["id"]
    df.to_csv(outputFilePath, index=False, encoding='utf-8')
