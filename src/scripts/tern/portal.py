import requests
import math
import pandas as pd
from pathlib import Path

def build(outputFilePath: Path) -> None:
    baseURL = "https://portal.tern.org.au/search/filter/"

    parameters = {
        "params": {
            "q": "*",
            "status": "PUBLISHED",
            "classFilter": "collection",
            "groupFilter": "All",
            "topicFilter": "All",
            "licenseFilter": "All",
            "typeFilter": "All",
            "subjectFilter": "All",
            "anzsrcforFilter": "All",
            "anzsrcseoFilter": "All",
            "gcmdFilter": "All",
            "temporalresFilter": "All",
            "verticalresFilter": "All",
            "horizontalresFilter": "All",
            "datagroupFilter": "All",
            "dataresolutionFilter": "All",
            "platformFilter": "All",
            "instrumentFilter": "All",
            "methodFilter": "All",
            "apniFilter": "All",
            "afdFilter": "All",
            "distributorFilter": "All",
            "authorFilter": "All",
            "datatypeFilter": "All",
            "ternRegionFilter": "All",
            "spatialPolygon": "",
            "temporal": "All",
            "sort": "_score desc",
            "page": 0,
            "num": 0,
            "mapsearch": "0"
        }
    }

    hitCountURL = "https://portal.tern.org.au/search/filter/TotalHitCountCollector/"
    hitCounts = requests.post(hitCountURL)
    hits = hitCounts.json()["json"]["total_docs"]
    hitsPerCall = 1000

    totalCalls = math.ceil(hits / hitsPerCall)

    parameters["params"]["num"] = hitsPerCall

    records = []
    for call in range(totalCalls):
        parameters["params"]["page"] = call + 1

        response = requests.post(baseURL, json=parameters)
        data = response.json()
        records.extend(data["json"]["hits"])
    
    df = pd.DataFrame.from_records(records)
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    df.to_csv(outputFilePath, index=False)