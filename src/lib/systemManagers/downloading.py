from pathlib import Path
from lib.systemManagers.baseManager import SystemManager, Task
from lib.processing.stages import File
from lib.processing.scripts import OutputScript
import logging
import lib.downloading as dl

class _Download(Task):
    def __init__(self, filePath: Path, properties: dict):
        self.file = File(filePath, properties)

    def getOutputPath(self) -> Path:
        return self.file.filePath

class _URLDownload(_Download):
    def __init__(self, url: str, filePath: Path, properties: dict, username: str, password: str):
        self.url = url
        self.auth = dl.buildAuth(username, password) if username else None

        super().__init__(filePath, properties)

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        if not overwrite and self.file.exists():
            logging.info(f"Output file {self.file.filePath} already exists")
            return False
        
        self.file.filePath.unlink(True)
        return dl.download(self.url, self.file.filePath, verbose=verbose, auth=self.auth)

class _ScriptDownload(_Download):
    def __init__(self, baseDir: Path, downloadDir: Path, scriptInfo: dict):
        self.script = OutputScript(baseDir, dict(scriptInfo), downloadDir)      

        super().__init__(self.script.output.filePath, self.script.output.fileProperties)

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        return self.script.run(overwrite, verbose)

class DownloadManager(SystemManager):
    def __init__(self, dataDir: Path, authFile: str):
        self.stepName = "downloading"

        super().__init__(dataDir.parent, self.stepName, "files")

        self.downloadDir = dataDir / self.stepName
        self.authFile = authFile

        authPath = self.baseDir / self.authFile
        if authFile and authPath.exists():
            with open(authPath) as fp:
                data = fp.read().rstrip("\n").split()

            self.username = data[0]
            self.password = data[1]

        else:
            self.username = ""
            self.password = ""

        self.downloads: list[_Download] = []

    def getFiles(self) -> list[File]:
        return [download.file for download in self.downloads]

    def getLatestFile(self) -> File:
        return self.files[-1].file

    def download(self, overwrite: bool = False, verbose: bool = False) -> bool:
        if not self.downloadDir.exists():
            self.downloadDir.mkdir(parents=True)

        return self.runTasks(self.downloads, overwrite, verbose)

    def registerFromURL(self, url: str, fileName: str, fileProperties: dict = {}) -> bool:
        download = _URLDownload(url, self.downloadDir / fileName, fileProperties, self.username, self.password)
        self.downloads.append(download)
        return True

    def registerFromScript(self, scriptInfo: dict) -> bool:
        try:
            download = _ScriptDownload(self.baseDir, self.downloadDir, scriptInfo)
        except AttributeError as e:
            logging.error(f"Invalid download script configuration: {e}")
            return False
        
        self.downloads.append(download)
        return True
