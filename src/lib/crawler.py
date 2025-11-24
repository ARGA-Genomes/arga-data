import re
import requests
import urllib.parse
from bs4 import BeautifulSoup
import concurrent.futures as cf
from pathlib import Path
import json
import logging
import lib.downloading as dl
import time
from lib.progressBar import ProgressBar
from lib.json import JsonSynchronizer

depthLimit = 100

class PageData:

    _dirStr = "directories"
    _fileStr = "files"

    def __init__(self, url: str, directoryLinks: list[str], fileLinks: list[str]):
        self.url = url
        self.directoryLinks = directoryLinks
        self.fileLinks = fileLinks

    def __eq__(self, other: 'PageData') -> bool:
        return self.url == other.url

    @classmethod
    def fromPackage(cls, url: str, links: dict) -> 'PageData':
        return cls(url, links.get(cls._dirStr, []), links.get(cls._fileStr, []))

    def package(self) -> dict[str, dict[str, list[str]]]:
        return {
            self.url: {
                self._dirStr: self.directoryLinks,
                self._fileStr: self.fileLinks
            }
        }
    
    def getFullSubDirs(self, baseURL: str = "") -> list[str]:
        return [urllib.parse.urljoin(baseURL or self.url, dirLink) for dirLink in self.directoryLinks]
    
    def getFullFiles(self, baseURL: str = "") -> list[str]:
        return [urllib.parse.urljoin(baseURL or self.url, fileLink) for fileLink in self.fileLinks]

class Crawler:

    _progressFile = "crawlerProgress.json"
    _metaSettings = "settings"
    _metaSettingURL = "url"
    _metaSettingRegex = "regex"
    _metaSettingDepth = "maxDepth"
    _metaProgress = "progress"

    def __init__(self, outputDir: Path, auth: dl.HTTPBasicAuth = None):
        self.outputDir = outputDir
        self.auth = auth

        self.session = None
        self.data = []

    def run(self, entryURL: str, fileRegex: str = None, maxDepth: int = -1, ignoreProgress: bool = False):
        if not self.outputDir.exists():
            self.outputDir.mkdir(parents=True)

        self.session = requests.Session()
        pattern = re.compile(fileRegex) if fileRegex is not None else None
        if maxDepth < 0:
            maxDepth = depthLimit

        metadata = JsonSynchronizer(self.outputDir / self._progressFile)
        if ignoreProgress:
            metadata.clear()

        savedSettings = metadata.get(self._metaSettings, {})
        currentSettings = {
            self._metaSettingURL: entryURL,
            self._metaSettingRegex: fileRegex,
            self._metaSettingDepth: maxDepth
        }

        for setting, value in currentSettings.items():
            if setting in savedSettings and value != savedSettings[setting]:
                metadata.clear()
                break

        metadata[self._metaSettings] = currentSettings
        previousPageData: list[list[PageData]] = metadata.get(self._metaProgress, [])

        if previousPageData:
            if len(previousPageData) >= maxDepth:
                return # Exit early if no crawling necessary
            
            previousPageData = [[PageData.fromPackage(url, links) for url, links in layer.items()] for layer in previousPageData]
            logging.info(f"Progress found, resuming crawling at depth: {len(previousPageData)}")
        else:
            previousPageData = [[self._getPageLinks(entryURL, pattern)]]
            logging.info(f"Successfully retrieved entry url {entryURL}, crawling subfolders")


        while len(previousPageData) <= maxDepth:
            folderURLs = [subDirURL for pageData in progress for subDirURL in pageData.getFullSubDirs()]
            if not folderURLs:
                break

            pageData = self._parallelPageLinks(folderURLs, pattern) [{key: value for pageData in layer for key, value in pageData.package().items()} for layer in self.data]
            metadata[self._metaProgress] += 

    def getFileURLs(self, altDLURL: str = "") -> list[str]:
        metadata = JsonSynchronizer(self.outputDir / self._progressFile)
        return [fileURL for layer in metadata.get(self._metaProgress, []) for pageData in layer for fileURL in pageData.getFullFiles(altDLURL)]

    def _parallelPageLinks(self, urlList: list[str], pattern: re.Pattern = None, retries: int = 5,) -> list[PageData]:
        data = []
        progress = ProgressBar(len(urlList), processName=f"Crawler Depth {len(self.data)}")
        with cf.ThreadPoolExecutor(max_workers=10) as executor:
            futures = (executor.submit(self._getPageLinks, url, pattern, retries) for url in urlList)
            for future in cf.as_completed(futures):
                result = future.result()
                progress.update()

                if result is None:
                    continue

                data.append(result)

        return data

    def _getPageLinks(self, url: str, filePattern: re.Pattern = None, retries: int = 5) -> PageData:
        if self.session is None:
            raise Exception("No session started") from ValueError

        for _ in range(retries):
            try:
                response = self.session.get(url, auth=self.auth)
                break
            except (ConnectionError, requests.exceptions.ConnectionError):
                time.sleep(0.5)
        else:
            return
        
        dirLinks = []
        fileLinks = []

        soup = BeautifulSoup(response.content, "html.parser")
        for hyperlink in soup.find_all("a"):
            link: str = hyperlink.get('href')

            if link is None or any(link.startswith(c) for c in ("/", "?")):
                continue

            if link.endswith("/"): # Subdirectory link
                dirLinks.append(link)
                continue

            # File url
            if filePattern is None:
                fileLinks.append(link)
                continue

            if filePattern.match(link):
                fileLinks.append(link)

        return PageData(url, dirLinks, fileLinks)
    
    def _save(self) -> None:
        data = [{key: value for pageData in layer for key, value in pageData.package().items()} for layer in self.data]

        with open(self.progressFile, "w") as fp:
            json.dump(data, fp, indent=4)
    
    def _load(self) -> list[list[PageData]]:
        if not self.progressFile.exists():
            return []
        
        with open(self.progressFile) as fp:
            data = json.load(fp)

        return [[PageData.fromPackage(url, links) for url, links in layer.items()] for layer in data]
