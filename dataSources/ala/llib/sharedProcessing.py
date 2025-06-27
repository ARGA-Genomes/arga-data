import requests
from pathlib import Path
import lib.downloading as dl
import logging
from lib.secrets import secrets
from lib.bigFileWriter import BigFileWriter

def collectBiocache(queryParamters: dict, outputFilePath: Path) -> None:
    paramters = {
        "email": secrets.general.email,
        "emailNotify": False
    }

    baseURL = "https://api.ala.org.au/occurrences/occurrences/offline/download?"
    url = dl.urlBuilder(baseURL, paramters | queryParamters)

    response = requests.get(url)
    data = response.json()

    statusURL = data["statusUrl"]
    totalRecords = data["totalRecords"]
    logging.info(f"Found {totalRecords} total records")

    dl.asyncRunner(statusURL, "status", "finished", "downloadUrl", outputFilePath)

def cleanup(folderPath: Path, outputFilePath: Path) -> None:
    extraFiles = [
        "citation.csv",
        "headings.csv",
        "README.html"
    ]

    for fileName in extraFiles:
        path = folderPath / fileName
        path.unlink(missing_ok=True)

    writer = BigFileWriter(outputFilePath)
    writer.populateFromFolder(folderPath)
    writer.oneFile()

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