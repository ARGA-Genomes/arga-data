from pathlib import Path
import requests
import pandas as pd
import math

def build(outputFilePath: Path, entriesPerPage: int) -> None:
    url = "https://data.bioplatforms.com/api/3/action/package_search?q=*:*&rows="

    initialRequest = requests.get(f"{url}{0}")
    initialJson = initialRequest.json()

    summary = initialJson.get("result", {})
    totalEntries = summary.get("count", 0)

    if totalEntries == 0:
        print("No entries found, quitting...")
        return
    
    numberOfCalls = math.ceil(totalEntries / entriesPerPage)

    entries = []
    for call in range(numberOfCalls):
        startEntry = call * entriesPerPage
        print(f"Reading page: {call+1} / {numberOfCalls}", end='\r')
        response = requests.get(f"{url}{entriesPerPage}&start={startEntry}")
        responseData = response.json()

        summary = responseData.get("result", {})
        results = summary.get("results", [])
        entries.extend(results)

    print()
    df = pd.DataFrame.from_records(entries)
    df.to_csv(outputFilePath, index=False, encoding='utf-8')
