from pathlib import Path
import requests
import math
import pandas as pd
import ast

def buildURL(keyword: str, perPage: int, page: int = 1) -> str:
    return f"https://collections.museumsvictoria.com.au/api/{keyword}?perpage={perPage}&page={page}"

def retrieve(dataset: str, outputFolder: Path, recordsPerPage: int) -> None:
    response = requests.get(buildURL(dataset, 1), headers={"User-Agent": ""})
    data = response.json()

    totalResults = int(response.headers.get("Total-Results", 0))
    totalCalls = math.ceil(totalResults / recordsPerPage)

    records = []
    for call in range(totalCalls):
        print(f"At call: {call+1}", end="\r")
        response = requests.get(buildURL(dataset, recordsPerPage, call+1), headers={"User-Agent": ""})
        data = response.json()
        records.extend(data)

    df = pd.DataFrame.from_records(records)
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    df.to_csv(outputFolder / f"{dataset}.csv", index=False)

def expandTaxa(filePath: Path, outputPath: Path) -> None:
    df = pd.read_csv(filePath)
    df2 = df["taxonomy"].fillna("{}")
    df2 = pd.json_normalize(df2.apply(ast.literal_eval))
    
    df.drop("taxonomy", axis=1, inplace=True)
    df = df.join(df2)
    df.to_csv(outputPath, index=False)
