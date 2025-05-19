import requests
from pathlib import Path
import lib.downloading as dl
import time
from urllib.parse import quote
import logging

def buildAVH(outputFilePath: Path) -> None:
    baseURL = "https://api.ala.org.au/occurrences/occurrences/offline/download?"
    fields = {
        "email": getEmail(),
        "emailNotify": False,
        "q": "*:*",
        "disableAllQualityFilters": True,
        "qualityProfile": "AVH",
        "fq": [
            "type_status:*",
            'country:"Australia"'
        ],
        "qc": "data_hub_uid:dh9"
    }

    url = buildURL(baseURL, fields)
    biocacheDownload(url, 10, outputFilePath)

def buildOzcam(outputFilePath: Path) -> None:
    baseURL = "https://api.ala.org.au/occurrences/occurrences/offline/download?"
    fields = {
        "email": getEmail(),
        "emailNotify": False,
        "q": "basis_of_record:OCCURRENCE PRESERVED_SPECIMEN MATERIAL_SAMPLE LIVING_SPECIMEN MATERIAL_CITATION",
        "disableAllQualityFilters": True,
        "qualityProfile": "ALA"
    }

    url = buildURL(baseURL, fields)
    biocacheDownload(url, 10, outputFilePath)

def getEmail() -> str:
    with open(Path(__file__).parent / "email.txt") as fp:
        return fp.read().rstrip("\n")

def buildURL(baseURL: str, fields: dict) -> str:
    def encode(key: str, value: any) -> str:
        if isinstance(value, bool):
            value = str(value).lower()

        if isinstance(value, int):
            value = str(value)

        return f"{key}={quote(value)}"

    flatFields = []
    for fieldName, value in fields.items():
        if isinstance(value, list):
            for item in value:
                flatFields.append(encode(fieldName, item))
            continue

        flatFields.append(encode(fieldName, value))

    return f"{baseURL}" + "&".join(flatFields)

def biocacheDownload(url: str, updateDelay: int, outputFilePath: Path) -> bool:
    session = requests.Session()

    def getJson(url: str) -> dict:
        response = session.get(url)
        return response.json()
    
    initialData = getJson(url)
    statusURL = initialData["statusUrl"]
    totalRecords = initialData["totalRecords"]
    logging.info(f"Found {totalRecords} total records")

    updateDelay = max(updateDelay, 5)
    loading = "|/-\\"
    ttl = 0
    
    statusData = getJson(statusURL)
    while statusData["status"] != "finished":
        for _ in range(updateDelay):
            for _ in range(2):
                print(f"> ({loading[ttl % len(loading)]}) Status: {statusData['status']}", end="\r")
            time.sleep(1)
            ttl += 1
        
        statusData = getJson(statusURL)
    
    downloadURL = statusData["downloadUrl"]
    return dl.download(downloadURL, outputFilePath, verbose=True)

# status = {
#     "inQueue": [
#         "totalRecords",
#         "queueSize",
#         "statusUrl",
#         "cancelUrl",
#         "searchUrl"
#     ],
#     "running": [
#         "totalRecords",
#         "records",
#         "statusUrl",
#         "cancelUrl",
#         "searchUrl"
#     ],
#     "finished": [
#         "totalRecords",
#         "queueSize",
#         "downloadUrl",
#         "statusUrl",
#         "cancelUrl",
#         "searchUrl"
#     ]
# }