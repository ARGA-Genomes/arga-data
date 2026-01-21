import requests
import pandas as pd
from pathlib import Path
import lib.secrets as scr
import lib.downloading as dl
from lib.progressBar import ProgressBar
import logging
import lib.dataframes as dff

def collect(outputDir: Path, profile: str) -> None:
    session = requests.session()
    secrets = scr.load()

    response = session.post(
        "https://auth.ala.org.au/cas/oidc/oidcAccessToken",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data="grant_type=client_credentials&scope=openid email profile roles",
        auth=dl.buildAuth(secrets.ala.id, secrets.ala.secret)
    )

    accessToken = response.json()["access_token"]

    baseURL = "https://api.ala.org.au/profiles"
    endpoint = f"/api/opus/{profile}/profile?pageSize=1000"
    response = requests.get(baseURL + endpoint, headers={"Authorization": f"Bearer {accessToken}"})
    data = response.json()

    if "message" in data and "not authorized" in data["message"]:
        logging.error("Failed to authorize, please make sure bearer token is valid.")
        return
    
    logging.info(f"Accessing profile: {profile}")

    dataLength = len(data)
    logging.info(f"Found {dataLength} records")

    progress = ProgressBar(dataLength)
    records = []
    for entry in data:
        uuid = entry["uuid"]
        response = requests.get(baseURL + f"/api/opus/{profile}/profile/{uuid}", headers={"Authorization": f"Bearer {accessToken}"})
        records.append(response.json())
        progress.update()

    df = pd.DataFrame.from_records(records)
    df = dff.removeSpaces(df)
    df.to_csv(outputDir / f"{profile}.csv", index=False)
