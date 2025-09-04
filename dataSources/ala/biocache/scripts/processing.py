import requests
from pathlib import Path
import lib.downloading as dl
import lib.networking as nw
import logging
from lib.secrets import secrets
import lib.bigFiles as bf

def collectBiocache(queryParamters: dict, outputFilePath: Path) -> None:
    paramters = {
        "email": secrets.general.email,
        "emailNotify": False
    }

    url = "https://api.ala.org.au/occurrences/occurrences/offline/download?"
    response = requests.get(url, params=nw.encodeParameters(paramters | queryParamters))
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

    bf.combineDirectoryFiles(outputFilePath, folderPath)

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
