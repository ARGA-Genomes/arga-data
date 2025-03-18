import requests
from pathlib import Path
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from lib.tools.logger import Logger
from lib.tools.progressBar import ProgressBar

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
        Logger.error(f"Invalid chunk size `{chunkSize}`, value must be greater than 0")
        return False
    
    if verbose:
        Logger.info(f"Downloading from {url} to file {filePath.absolute()}")
        progressBar = ProgressBar(processName="Downloading")

    try:
        requests.head(url, auth=auth, headers=headers)
    except requests.exceptions.InvalidSchema as e:
        Logger.error(f"Schema error: {e}")
        return False

    with requests.get(url, stream=True, auth=auth, headers=headers) as stream:
        try:
            stream.raise_for_status()
        except HTTPError:
            Logger.error("Received HTTP error")
            return False
        
        fileSize = int(stream.headers.get("Content-Length", 0))

        with open(filePath, "wb") as fp:
            for idx, chunk in enumerate(stream.iter_content(chunkSize), start=1):
                fp.write(chunk)

                if not verbose:
                    continue
                
                if fileSize > 0: # File size known, can render completion %
                    progressBar.update((idx * chunkSize) / fileSize)
                else:
                    print(f"Downloaded chunk: {idx}", end="\r")

    if verbose:                
        print()

    return True
