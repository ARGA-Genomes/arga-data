import requests
from pathlib import Path
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
import logging
from lib.progressBar import ProgressBar
from urllib.parse import quote
import time

class RepeatDownloader:
    def __init__(self, headers: dict = {}, username: str = "", password: str = "", chunkSize: int = 1024*1024, verbose: bool = False):
        self.headers = headers
        self.auth = buildAuth(username, password) if username else None
        self.chunkSize = chunkSize
        self.verbose = verbose

    def download(self, url: str, filePath: Path, customChunkSize: int = -1, additionalHeaders: dict = {}) -> bool:
        chunkSize = customChunkSize if customChunkSize >= 0 else self.chunkSize
        return download(url, filePath, chunkSize, self.verbose, self.headers | additionalHeaders, self.auth)

def download(url: str, filePath: Path, chunkSize: int = 1024*1024, verbose: bool = False, headers: dict = {}, auth: HTTPBasicAuth = None) -> bool:
    if chunkSize <= 0:
        logging.error(f"Invalid chunk size `{chunkSize}`, value must be greater than 0")
        return False
    
    if verbose:
        logging.info(f"Downloading from {url} to file {filePath.absolute()}")

    try:
        requests.head(url, auth=auth, headers=headers)
    except requests.exceptions.InvalidSchema as e:
        logging.error(f"Schema error: {e}")
        return False

    with requests.get(url, stream=True, auth=auth, headers=headers) as stream:
        try:
            stream.raise_for_status()
        except HTTPError:
            logging.error("Received HTTP error")
            return False
        
        fileSize = int(stream.headers.get("Content-Length", 0))
        if verbose and fileSize > 0:
            progressBar = ProgressBar((fileSize / chunkSize).__ceil__(), processName="Downloading")

        with open(filePath, "wb") as fp:
            for idx, chunk in enumerate(stream.iter_content(chunkSize), start=1):
                fp.write(chunk)

                if not verbose:
                    continue
                
                if fileSize > 0:
                    progressBar.update()
                else:
                    print(f"Downloaded chunk: {idx}", end="\r")

    return True

def asyncRunner(checkURL: str, statusField: str, completedStr: str, downloadField: str, outputFilePath: Path, recheckDelay: int = 10) -> bool:
    session = requests.Session()

    def getCompleted() -> tuple[bool, str, str]:
        response = session.get(checkURL)
        if response.status_code != 200:
            logging.warning(f"Failed to retrieve {checkURL}, received status code {response.status_code}. Reason: {response.reason}")
            return True, None, None
        
        data = response.json()

        statusValue = data.get(statusField, "Unknown")
        downloadURL = data.get(downloadField, "")

        return statusValue == completedStr, statusValue, downloadURL
    
    loading = "|/-\\"
    totalChecks = 0
    recheckDelay = max(recheckDelay, 5)
    reprintsPerSecond = 2

    logging.info(f"Polling {checkURL} for status...")
    completed, status, downloadURL = getCompleted()
    while not completed:
        for _ in range(recheckDelay):
            for _ in range(reprintsPerSecond):
                print(f"> ({loading[totalChecks % len(loading)]}) Status: {status}", end="\r")
                time.sleep(1 / reprintsPerSecond)

            totalChecks += 1

        completed, status, downloadURL = getCompleted()

    if status is None:
        logging.error("Failed to check status of download.")
        return False

    return download(downloadURL, outputFilePath, verbose=True)
