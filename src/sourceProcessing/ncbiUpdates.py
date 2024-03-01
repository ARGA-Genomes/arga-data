from pathlib import Path
from lib.sourceObjs.timeManager import TimeManager
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import lib.config as cfg
from lib.tools.extractor import Extractor
import lib.commonFuncs as cmn
from lib.tools.logger import Logger

class UpdateManager:
    class Release:
        def __init__(self, version: int, href: str, timestamp: datetime):
            self.version = version
            self.href = href
            self.timestamp = timestamp

        def __repr__(self) -> str:
            return f"{self.version}: {self.timestamp}"

    def __init__(self):
        self.ncbiFolder = cfg.Folders.dataSources / "ncbi"
        self.saveFile = "nucData.json"

        self.baseURL = "https://ftp.ncbi.nlm.nih.gov/genbank/"
        self.releaseNumURL = "GB_Release_Number"
        self.releaseNotesURL = "release.notes"

        self.new = "gbnew.txt.gz"
        self.changed = "gbchg.txt.gz"
        self.deleted = "gbdel.txt.gz"

    def getReleases(self) -> list[Release]:
        data = requests.get(self.baseURL + self.releaseNumURL)
        releaseNum = data.text.rstrip("\n")

        data = requests.get(self.baseURL + self.releaseNotesURL)
        soup = BeautifulSoup(data.text, "html.parser")

        releases = []
        for element in soup.find_all("a"):
            if not "release.notes" in element.get("href", ""):
                continue

            version = element["href"].split(".")[0].strip("gb")
            if version == "README":
                version = releaseNum

            date, time = element.next_sibling.strip().split(" ")[:2]
            year, month, day = date.split("-")
            hour, minute = time.split(":")
            dt = datetime(int(year), int(month), int(day), int(hour), int(minute)) + timedelta(hours=16) # Offset time for timezones
            releases.append(self.Release(int(version), element["href"], dt))

        return sorted(releases, key=lambda x: x.version, reverse=True)
    
    def _parse(self, filePath: Path, prefix: str) -> dict[str, list[str]]:
        data: dict[str, list[str]] = {}
        with open(filePath) as fp:
            rawData = fp.read().rstrip("\n").split("\n")

        for line in rawData:
            file, loci = line.split("|")
            if prefix and not file.startswith(prefix):
                continue

            if file not in data:
                data[file] = []

            data[file].append(loci)
    
        return data
    
    def getUpdates(self, prefix: str = "") -> list[dict[str, list[str]]]:
        data = []
        for file in (self.new, self.changed, self.deleted):
            url = self.baseURL + file
            localFile = self.ncbiFolder / file

            if not localFile.exists():
                cmn.downloadFile(url, localFile, verbose=False)

            extractedFile = Extractor.extract(localFile)
            data.append(self._parse(extractedFile, prefix))

        return data

def updateNucleotide(basePath: Path, ncbiPrefix: str) -> tuple[bool, Path]:
    timeManager = TimeManager(basePath)
    lastUpdate, _ = timeManager.getLastUpdate()
    if lastUpdate is None:
        Logger.info("No last update")
        return False, ""

    updateManager = UpdateManager()
    
    updates = []
    for release in updateManager.getReleases():
        if (lastUpdate - release.timestamp).total_seconds() >= 0: # If timestamp is older than last update
            break

        updates.append(release)

    if not updates: # No updates are required, return success but no new update path
        Logger.info("No updates required")
        return True, ""
    
    if len(updates) > 1: # If there are multiple updates required, don't use update script
        Logger.info("Too many updates to dynamically update")
        return False, ""
    
    prefix = ncbiPrefix.upper()
    new, changed, deleted = updateManager.getUpdates(prefix)

    print(new, changed, deleted)
