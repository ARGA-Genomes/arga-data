from pathlib import Path
import lib.downloading as dl
from lib.secrets import Secrets
import requests
from lib.processing.scripts import importableScript
from lib.processing.files import DataFile

@importableScript(inputCount=0)
def collect(outputDir: Path) -> None:
    secrets = Secrets("gbif")

    baseURL = "https://api.gbif.org/v1"
    requestEndpoint = "/occurrence/download/request"
    statusEndpoint = "/occurrence/download"

    headers = {
        "Content-Type": "application/json"
    }

    formData = {
        "creator": secrets.username,
        "notificationAddresses": [
            secrets.email
        ],
        "sendNotification": "false",
        "format": "SIMPLE_CSV",
        "predicate": {
            "type": "and",
            "predicates": [
                {
                    "type": "in",
                    "key": "BASIS_OF_RECORD",
                    "values": [
                        "MATERIAL_SAMPLE",
                        "MATERIAL_CITATION",
                        "PRESERVED_SPECIMEN",
                        "FOSSIL_SPECIMEN",
                        "LIVING_SPECIMEN",
                        "OCCURRENCE"
                    ]
                },
                {
                    "type": "equals",
                    "key": "COUNTRY",
                    "value": "AU"
                },
                {
                    "type": "equals",
                    "key": "HAS_COORDINATE",
                    "value": "true"
                },
                {
                    "type": "equals",
                    "key": "HAS_GEOSPATIAL_ISSUE",
                    "value": "false"
                },
                {
                    "type": "equals",
                    "key": "OCCURRENCE_STATUS",
                    "value": "PRESENT"
                },
            ]
        }
    }

    strFormData = str(formData).replace(" ", "").replace("'", '"')
    response = requests.post(f"{baseURL}{requestEndpoint}", headers=headers, data=strFormData, auth=dl.buildAuth(secrets.email, secrets.password))
    requestID = response.text

    statusURL = f"{baseURL}{statusEndpoint}/{requestID}"
    dl.asyncRunner(statusURL, "status", "SUCCEEDED", "downloadLink", outputDir / "gbif.zip")
