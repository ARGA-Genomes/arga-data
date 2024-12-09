from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lib.tools.progressBar import SteppableProgressBar

def getMetadata(filePath: Path, outputFile: Path):
    outputFolder = outputFile.parent / "metadata"
    outputFolder.mkdir(exist_ok=True)

    # df = pd.read_csv(filePath, sep="\t")
    # session = requests.Session()

    # ids = df[df["URL"].notna()]
    # progress = SteppableProgressBar(50, len(ids), "Scraping")

    # for _, row in ids.iterrows():
    #     url = row["URL"]
    #     id = row["ID"]

    #     outputFile = outputFolder / f"{id}.txt"
    #     if not outputFile.exists():
    #         response = session.get(url)
    #         soup = BeautifulSoup(response.content, "html.parser")
    #         content = soup.find("p", {"class": "result"})

    #         with open(outputFile, "w", encoding="utf-8") as fp:
    #             fp.write(content.text)

    #     progress.update()

    amount = 4
    records = []
    for idx, file in enumerate(outputFolder.iterdir()):
        with open(file, encoding="utf-8") as fp:
            data = fp.read()

        # if file.stem.startswith("G"):
        #     continue

        print(f"Running file: {file}")
        record = (parseGenus if file.stem.startswith("G") else parseSpecies)(file.stem, data)
        print(record)
        records.append(record)
        
        if idx == amount:
            break

def parseGenus(id: str, data: str) -> dict:
    record = {"id": id}

    frontHalf, backHalf = data.split("•", 1)
    genus, frontHalf = frontHalf.split(" ", 1)
    record["genus"] = genus

    authorYear, typeInfo = frontHalf.split(":", 1)
    authorInfo, year = authorYear.rsplit(" ", 1) 
    record["year"] = year

    authors = []
    nextAuthorEnd = authorInfo.find("]")
    while nextAuthorEnd > 0:
        authors.append(authorInfo[:nextAuthorEnd].strip(" &"))
        authorInfo = authorInfo[nextAuthorEnd+1:]
        nextAuthorEnd = authorInfo.find("]")

    record["author"] = " & ".join(authors)
    record["simpleAuthor"] = " & ".join(author.split("[")[0].strip() for author in authors)
    record["in"] = authorInfo.strip(" in")

    pageGender, typeInfo = typeInfo.split(".", 1)
    page, gender = pageGender.rsplit(" ", 1)
    record["page"] = page
    record["genderVerbatim"] = f"{gender}."
    record["gender"] = {"Fem": "feminine", "Masc": "masculine", "Neut": "neuter"}.get(gender)

    typeInfo = typeInfo.strip(" .").split(". ", 2)
    typeInfo += ["" for _ in range(3 - len(typeInfo))]
    record["typeSpecies"], record["typification"], record["typeJustification"] = typeInfo

    record["scientificName"] = f"{genus} {record['simpleAuthor']}, {year}"

    references, currentStatus = backHalf[9:].split("Current status: Valid as ")
    currentStatus, familyStatus = currentStatus.rstrip(" .").split(". ")
    record["currentStatus"] = currentStatus
    record["acceptedAs"] = currentStatus[::-1].replace(" ", " ,", 1)[::-1]
    familyStatus = familyStatus.split(": ")
    familyStatus += ["" for _ in range(2 - len(familyStatus))]
    record["family"], record["subfamily"] = familyStatus

    return record

def parseSpecies(id: str, data: str) -> dict:
    record = {"id": id}

    frontHalf, backHalf = data.split("•", 1)
    specificEpithet, genus, frontHalf = frontHalf.split(" ", 2)
    record["specificEpithet"] = specificEpithet.strip(", ")
    record["genus"] = genus

    authorYear, typeInfo = frontHalf.split(":", 1)
    authorInfo, year = authorYear.rsplit(" ", 1) 
    record["year"] = year

    authors = []
    nextAuthorEnd = authorInfo.find("]")
    while nextAuthorEnd > 0:
        authors.append(authorInfo[:nextAuthorEnd].strip(" &"))
        authorInfo = authorInfo[nextAuthorEnd+1:]
        nextAuthorEnd = authorInfo.find("]")

    page, typeInfo = typeInfo.split(" [", 1)
    record["page"] = page
    origDescription, typeInfo = typeInfo.split("] ", 1)
    record["originalDescription"] = origDescription
    typeInfo, types = typeInfo.split(". ", 1)

    if typeInfo.endswith("E") or typeInfo.endswith("W"):
        typeInfo, ns, ew = typeInfo.rsplit(", ", 2)
        record["coordsVerbatim"] = f"{ns}, {ew}"
    else:
        record["coordsVerbatim"] = ""

    record["typeLocation"] = typeInfo
    record["digitalCoordSystem"] = "WGS 84"
    return record
