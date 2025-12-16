from pathlib import Path
import requests
import pandas as pd
import ast
from lib.progressBar import ProgressBar
from lib.bigFiles import RecordWriter
import logging

def retrieve(dataset: str, outputFolder: Path, recordsPerPage: int) -> None:
    session = requests.Session()

    def getRecords(pageNum: int) -> requests.Response:
        url = f"https://collections.museumsvictoria.com.au/api/{dataset}?perpage={recordsPerPage}&page={pageNum}"
        return session.get(url, headers={"User-Agent": ""})

    def flattenPageData(pageData: list[dict]) -> list[dict]:
        def flattenRecord(record: dict) -> dict:
            flatRecord = {}

            for key, value in record.items():
                if not isinstance(value, dict):
                    flatRecord[key] = value
                    continue

                for subKey, subValue in value.items():
                    flatRecord[f"{key}_{subKey}"] = subValue
            
            return flatRecord

        return [flattenRecord(record) for record in pageData]

    response = getRecords(1)
    totalResults = int(response.headers.get("Total-Results", 0))
    if totalResults == 0:
        logging.error(f"Unable to retrieve dataset {dataset}")
        return

    totalCalls = (totalResults / recordsPerPage).__ceil__()
    progress = ProgressBar(totalCalls - 1)
    writer = RecordWriter(outputFolder / f"{dataset}.csv", 100000)

    data = response.json()
    writer.writerMultipleRecords(flattenPageData(data))

    for call in range(2, totalCalls+1):
        response = getRecords(call)
        data = response.json()
        writer.writerMultipleRecords(flattenPageData(data))
        progress.update()

    writer.combine(removeParts=True)

def expandTaxa(filePath: Path, outputPath: Path) -> None:
    df = pd.read_csv(filePath)
    df2 = df["taxonomy"].fillna("{}")
    df2 = pd.json_normalize(df2.apply(ast.literal_eval))
    
    df.drop("taxonomy", axis=1, inplace=True)
    df = df.join(df2)
    df.to_csv(outputPath, index=False)
