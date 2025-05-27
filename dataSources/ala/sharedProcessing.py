import requests
from pathlib import Path
import lib.downloading as dl
import time
from urllib.parse import quote
import logging
from lib.secrets import secrets
import lib.common as cmn
from lib.bigFileWriter import BigFileWriter

paramterSets = {
    "avh": {
        "email": secrets.general.email,
        "emailNotify": False,
        "q": "*:*",
        "disableAllQualityFilters": True,
        "qualityProfile": "AVH",
        "fq": [
            "type_status:*",
            'country:"Australia"'
        ],
        "qc": "data_hub_uid:dh9"
    },
    "ozcam": {
        "email": secrets.general.email,
        "emailNotify": False,
        "q": "basis_of_record:OCCURRENCE PRESERVED_SPECIMEN MATERIAL_SAMPLE LIVING_SPECIMEN MATERIAL_CITATION",
        "disableAllQualityFilters": True,
        "qualityProfile": "ALA"
    }
}

def collect(parameterSet: str, outputFilePath: Path) -> None:
    paramters = paramterSets.get(parameterSet, None)
    if paramters is None:
        raise Exception(f"Unspecified paramter set: {parameterSet}") from AttributeError

    baseURL = "https://api.ala.org.au/occurrences/occurrences/offline/download?"
    url = dl.urlBuilder(baseURL, paramters)

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