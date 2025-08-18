from pathlib import Path
import pandas as pd
import requests

def collect(outputPath: Path) -> None:
    baseURL = "https://lists-ws.test.ala.org.au/"
    session = requests.Session()
    recordsPerPage = 100
    
    def getURL(endpoint: str, params: dict, pageSize: int, page: int = 1) -> dict:
        fields = dict(params)
        fields["page"] = page
        fields["pageSize"] = pageSize

        url = f"{baseURL}{endpoint}?" + "&".join(f"{k}={v}" for k, v in fields.items())
        response = session.get(url)
        data = response.json()
        return data
    
    listsMetadata = outputPath.parent / "metadata.csv"
    if not listsMetadata.exists():
        records = []
        metadataEndpoint = "speciesList/"
        
        query = {"tag": "arga"}
        data = getURL(metadataEndpoint, query, recordsPerPage)
        records.extend(data["lists"])
        totalItems = data["listCount"]
        remainingCalls = ((totalItems / recordsPerPage).__ceil__()) - 1
        
        for call, _ in enumerate(range(remainingCalls), start=2):
            data = getURL(metadataEndpoint, query, recordsPerPage, call)
            records.extend(data["lists"])

        df = pd.DataFrame.from_records(records)
        df = df.drop(["description"], axis=1)
        df.to_csv(listsMetadata, index=False)
    else:
        df = pd.read_csv(listsMetadata)

    records = []
    for id in df["id"]:
        page = 1
        while True:
            print(f"Getting page #{page} for id {id}", end="\r")
            data = getURL(f"speciesListItems/{id}", {}, recordsPerPage, page)
            if not data:
                break

            records.extend(data)
            page += 1

        print()
        
    df2 = pd.DataFrame.from_records(records)
    df = df.rename(columns={"id": "speciesListID", "version": "speciesListVersion"})
    df = df.merge(df2, "outer", on="speciesListID")
    df2.to_csv(outputPath, index=False)
