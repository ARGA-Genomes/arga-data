import requests
from pathlib import Path
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
import logging
from lib.progressBar import ProgressBar

class RepeatDownloader:
    def __init__(self, headers: dict = {}, username: str = "", password: str = "", chunkSize: int = 1024*1024, verbose: bool = False):
        self.headers = headers
        self.auth = buildAuth(username, password) if username else None
        self.chunkSize = chunkSize
        self.verbose = verbose

    def download(self, url: str, filePath: Path, customChunkSize: int = -1, additionalHeaders: dict = {}) -> bool:
        chunkSize = customChunkSize if customChunkSize >= 0 else self.chunkSize
        return download(url, filePath, chunkSize, self.verbose, self.headers | additionalHeaders, self.auth)

def buildAuth(username: str, password: str) -> HTTPBasicAuth:
    return HTTPBasicAuth(username, password)

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
