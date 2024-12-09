from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lib.tools.progressBar import SteppableProgressBar
import json

def getMetadata(filePath: Path, outputFile: Path):
    metadataFolder = outputFile.parent / "metadata"
    metadataFolder.mkdir(exist_ok=True)

    # df = pd.read_csv(filePath, sep="\t")
    # session = requests.Session()

    # ids = df[df["URL"].notna()]
    # progress = SteppableProgressBar(50, len(ids), "Scraping")

    # for _, row in ids.iterrows():
    #     url = row["URL"]
    #     id = row["ID"]

    #     file = metadataFolder / f"{id}.txt"
    #     if not file.exists():
    #         response = session.get(url)
    #         soup = BeautifulSoup(response.content, "html.parser")
    #         content = soup.find("p", {"class": "result"})

    #         with open(outputFile, "w", encoding="utf-8") as fp:
    #             fp.write(content.text)

    #     progress.update()

    parsedFolder = outputFile.parent / "parsedMetadata"
    parsedFolder.mkdir(exist_ok=True)

    records = []
    progress = SteppableProgressBar(50, len(list(metadataFolder.glob("*.txt"))), "Parsing")
    for file in metadataFolder.iterdir():
        newFilePath = parsedFolder / f"{file.stem}.json"

        if not newFilePath.exists():
            with open(file, encoding="utf-8") as fp:
                data = fp.read()

            try:
                record = (parseGenus if file.stem.startswith("G") else parseSpecies)(file.stem, data)
            except:
                print(f"\nError with {file}")
                # print(data)
                exit()

            with open(newFilePath, "w") as fp:
                json.dump(record, fp, indent=4)

        else:
            # with open(newFilePath) as fp:
            #     record = json.load(fp)
            pass

        # records.append(record)
        progress.update()

    df2 = pd.DataFrame.from_records(records)
    print(df.columns, df2.columns)

def parseGenus(id: str, data: str) -> dict:
    record = {"id": id}

    data = data.replace("  ", "•")
    frontHalf, backHalf = data.split("•", 1)
    genus, frontHalf = frontHalf.split(" ", 1)
    record["genus"] = genus

    genders = {"Fem.": "feminine", "Masc.": "masculine", "Neut.": "neuter"}
    for gender in genders:
        if frontHalf.find(gender) >= 0:
            break

    record["genderVerbatim"] = gender
    record["gender"] = genders.get(gender)

    authorsPage, typeInfo = frontHalf.split(gender)

    authors = []
    nextAuthorEnd = authorsPage.find("]")
    while nextAuthorEnd > 0:
        authors.append(authorsPage[:nextAuthorEnd+1].strip(" &"))
        authorInfo = authorsPage[nextAuthorEnd+1:]
        nextAuthorEnd = authorsPage.find("]", nextAuthorEnd+1)

    record["author"] = " & ".join(authors)
    record["simpleAuthor"] = " & ".join(author.split("[")[0].strip() for author in authors)
    record["in"] = authorInfo.strip(" in")

    yearPage = authorsPage.strip().split(":", 1)
    yearPage += ["" for _ in range(2 - len(yearPage))]
    record["year"], record["page"] = yearPage

    typeInfo = typeInfo.strip(" .").split(". ", 2)
    typeInfo += ["" for _ in range(3 - len(typeInfo))]
    record["typeSpecies"], record["typification"], record["typeJustification"] = typeInfo

    record["scientificName"] = f"{genus} {record['simpleAuthor']}, {record['year']}"

    references, currentStatus = backHalf.split("Current status: ")
    currentStatus = currentStatus.rstrip(" .").rsplit(". ", 1)
    currentStatus += ["" for _ in range(2 - len(currentStatus))]
    currentStatus, familyStatus = currentStatus
    currentStatus, _, currentName = currentStatus.split(" ", 2)
    
    record["currentStatus"] = currentStatus
    record["acceptedAs"] = currentName[::-1].replace(" ", " ,", 1)[::-1]
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
    locationInfo, types = typeInfo.split(". ", 1)

    if locationInfo.endswith("E") or locationInfo.endswith("W"):
        locationInfo, ns, ew = locationInfo.rsplit(", ", 2)
        record["coordsVerbatim"] = f"{ns}, {ew}"
    else:
        record["coordsVerbatim"] = ""

    record["typeLocation"] = locationInfo
    record["digitalCoordSystem"] = "WGS 84"

    types = types.rstrip(". ").split(". ", 1)
    types += ["" for _ in range(2 - len(types))]
    typeStyle, otherTypes = types
    typeStyle, typeRegistration = typeStyle.split(":")
    record["typeStyle"] = typeStyle
    record["typeRegistration"] = typeRegistration
    record["otherTypes"] = otherTypes

    references, currentStatus = backHalf.split("Current status: Valid as ")
    currentStatus, family, otherFields = currentStatus.rstrip(". ").split(". ", 2)
    record["currentValidName"] = currentStatus

    family = family.split(":")
    family += ["" for _ in range(2 - len(family))]
    record["family"], record["subfamily"] = family

    for field in otherFields.split(". "):
        label, value = field.split(":", 1)
        record[label.lower().strip()] = value.strip(". ")

    return record
