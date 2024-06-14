import requests
from pathlib import Path
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from lib.tools.logger import Logger
from lib.tools.progressBar import ProgressBar

class Downloader:
    def __init__(self, defaultChunksize: int = 1024*1024, username: str = "", password: str = ""):
        self.defaultChunksize = defaultChunksize
        self.auth = HTTPBasicAuth(username, password) if username else None

        self.progressBar = ProgressBar(50, "Downloading")

    def download(self, url: str, filePath: Path, customChunksize: int = -1, verbose: bool = False) -> bool:
        if verbose:
            Logger.info(f"Downloading from {url} to file {filePath}")

        with requests.get(url, stream=True, auth=self.auth) as stream:
            try:
                stream.raise_for_status()
            except HTTPError:
                Logger.error("Received HTTP error")
                return False
            
            fileSize = int(stream.headers.get("Content-Length", 0))
            chunksize = customChunksize if customChunksize > 0 else self.defaultChunksize

            with open(filePath, "wb") as fp:
                for idx, chunk in enumerate(stream.iter_content(chunksize), start=1):
                    fp.write(chunk)

                    if fileSize > 0: # File size known, can render completion %
                        self.progressBar.render((idx * chunksize) / fileSize)
                    else:
                        print(f"Downloaded chunk: {idx}", end="\r")

        return True
