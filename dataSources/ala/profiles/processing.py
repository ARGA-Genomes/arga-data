import json
import requests
from pathlib import Path
import lib.dataframeFuncs as dff
import pandas as pd

def collect(outputDir: Path, profile: str, tokenFilePath: Path) -> None:
    with open(tokenFilePath) as fp:
        token = json.load(fp)

    bearerToken = token["access_token"]
    baseURL = "https://api.ala.org.au/profiles"
    endpoint = f"/api/opus/{profile}/profile?pageSize=1000"
    response = requests.get(baseURL + endpoint, headers={"Authorization": f"Bearer {bearerToken}"})
    data = response.json()

    if "message" in data and "not authorized" in data["message"]:
        print("Failed to authorize, please make sure bearer token is valid.")
        return
    
    print(f"Accessing profile: {profile}")

    records = []
    for idx, entry in enumerate(data, start=1):
        uuid = entry["uuid"]
        print(f"At record: {idx}", end="\r")

        response = requests.get(baseURL + f"/api/opus/{profile}/profile/{uuid}", headers={"Authorization": f"Bearer {bearerToken}"})
        records.append(response.json())
    print()

    df = pd.DataFrame.from_records(records)
    df = dff.removeSpaces(df)
    df.to_csv(outputDir / f"{profile}.csv", index=False)