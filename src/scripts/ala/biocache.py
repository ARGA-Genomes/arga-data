import requests
from pathlib import Path
import lib.downloading as dl
import logging
from lib.secrets import Secrets
import lib.bigFiles as bf
from lib.processing.scripts import importableScript
from lib.processing.files import DataFile
import lib.zipping as zp

@importableScript(inputCount=0)
def collectBiocache(outputDir: Path, queryParamters: dict) -> None:
    secrets = Secrets()

    paramters = {
        "email": secrets.email,
        "emailNotify": False
    }

    baseURL = "https://api.ala.org.au/occurrences/occurrences/offline/download?"
    url = dl.urlBuilder(baseURL, paramters | queryParamters)

    response = requests.get(url)
    data = response.json()

    statusURL = data["statusUrl"]
    totalRecords = data["totalRecords"]
    logging.info(f"Found {totalRecords} total records")

    dl.asyncRunner(statusURL, "status", "finished", "downloadUrl", outputDir / "biocache.csv")

@importableScript()
def cleanup(outputDir: Path, inputFile: DataFile) -> None:
    extractedFolder = zp.extract(inputFile.path, outputDir)

    extraFiles = [
        "citation.csv",
        "headings.csv",
        "README.html"
    ]

    for fileName in extraFiles:
        path = extractedFolder / fileName
        path.unlink(missing_ok=True)

    bf.combineDirectoryFiles(outputDir / "compiledBiocache.csv", extractedFolder)

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
