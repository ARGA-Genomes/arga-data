from pathlib import Path
import lib.downloading as dl
import lib.secrets as scr
import requests

def collect(outputFilePath: Path) -> None:
    secrets = scr.load()

    baseURL = "https://api.gbif.org/v1"
    requestEndpoint = "/occurrence/download/request"
    statusEndpoint = "/occurrence/download"

    headers = {
        "Content-Type": "application/json"
    }

    formData = {
        "creator": secrets.gbif.creator,
        "notificationAddresses": [
            secrets.general.email
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
    response = requests.post(f"{baseURL}{requestEndpoint}", headers=headers, data=strFormData, auth=dl.buildAuth(secrets.general.email, secrets.gbif.password))
    requestID = response.text

    statusURL = f"{baseURL}{statusEndpoint}/{requestID}"
    dl.asyncRunner(statusURL, "status", "SUCCEEDED", "downloadLink", outputFilePath)
