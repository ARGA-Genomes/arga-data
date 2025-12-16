import requests
from pathlib import Path
import pandas as pd
from lib.bigFiles import RecordWriter
import time
import lib.dataframes as dff
from lib.secrets import secrets

def retrieve(outputFilePath: Path):
    baseURL = "https://api.iucnredlist.org/api/v4"
    headers = {
        "accept": "application/json",
        "Authorization": secrets.iucn.key
    }

    session = requests.Session()

    # Get version
    response = session.get(f"{baseURL}/information/red_list_version", headers=headers)
    version = list(response.json().values())[0]
    print(f"Version: {version}")
    
    writer = RecordWriter(outputFilePath, 10000)
    onPage = writer.writtenFileCount() + 1

    running = True
    while running:
        for _ in range(6):
            response = session.get(f"{baseURL}/scopes/1?page={onPage}&latest=true", headers=headers)
            if response.text.startswith("Retry later"):
                time.sleep(5)
                continue

            data: dict = response.json()
            assessmentIDs = [assessment["assessment_id"] for assessment in data["assessments"]]
            break
        else:
            print("Failed\n\n")
            return

        if len(assessmentIDs) < 100 or not assessmentIDs:
            running = False

        for idx, assessmentID in enumerate(assessmentIDs, start=1):
            print(f"Processing assessment ID #{((onPage - 1) * 100) + idx}: {assessmentID}", end= "\r")
            response = session.get(f"{baseURL}/assessment/{assessmentID}", headers=headers)
            try:
                data: dict = response.json()
            except requests.exceptions.JSONDecodeError:
                continue

            taxonomy = data.pop("taxon")

            commonNames = taxonomy.pop("common_names")
            taxonomy["common_names"] = []
            for name in commonNames:
                if isinstance(name["language"], dict):
                    name["language"] = name["language"]["description"]["en"]
                taxonomy["common_names"].append(name)

            # Flatten description from following items
            for item in ("population_trend", "red_list_category", "biogeographical_realms", "systems"):
                if isinstance(data[item], dict):
                    data[item] = data[item]["description"]["en"]

                if isinstance(data[item], list):
                    data[item] = [element["description"]["en"] for element in data[item]]

            supplementaryInfo = data.pop("supplementary_info")

            # Remove scopes
            data.pop("scopes")

            writer.write(data | taxonomy | supplementaryInfo)

        onPage += 1

    writer.combine(removeParts=True)
    print()

def reduce(filePath: Path, outputFilePath: Path) -> None:
    def filter(field: str) -> bool:
        if not isinstance(field, str) or not (field.startswith("[") and field.endswith("]")):
            return False
        
        validValues = [
            "Antarctic",
            "Australasian",
            "Oceanian",
            "Indomalayan"
        ]

        values = field.strip("[']").split("' '")
        return any(value in validValues for value in values)

    df = pd.read_csv(filePath, low_memory=False)
    df = df[df["biogeographical_realms"].apply(lambda x: filter(x))]
    df = dff.removeSpaces(df)
    df.to_csv(outputFilePath, index=False)
