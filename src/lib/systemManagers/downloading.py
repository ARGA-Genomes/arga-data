from pathlib import Path
from lib.systemManagers.baseManager import SystemManager, Task
from lib.processing.stages import File
from lib.processing.scripts import OutputScript
import logging
import lib.downloading as dl

class _Download(Task):
    def __init__(self, filePath: Path, properties: dict):
        super().__init__()
        
        self.file = File(filePath, properties)

    def getOutputPath(self) -> Path:
        return self.file.filePath

class _URLDownload(_Download):
    def __init__(self, url: str, filePath: Path, properties: dict, auth: dl.HTTPBasicAuth):
        self.url = url
        self.auth = auth

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
        return self.script.run(overwrite, verbose)[0] # No retval for downloading tasks, just return success

class DownloadManager(SystemManager):
    def __init__(self, baseDir: Path, dataDir: Path, username: str, password: str):
        self.stepName = "downloading"
        super().__init__(baseDir, dataDir, self.stepName, "files")

        self.auth = dl.buildAuth(username, password) if username else None
        self.downloads: list[_Download] = []

    def getFiles(self) -> list[File]:
        return [download.file for download in self.downloads]

    def getLatestFile(self) -> File:
        return self.files[-1].file

    def download(self, overwrite: bool = False, verbose: bool = False) -> bool:
        return self.runTasks(self.downloads, overwrite, verbose)

    def registerFromURL(self, url: str, fileName: str, fileProperties: dict = {}) -> bool:
        download = _URLDownload(url, self.workingDir / fileName, fileProperties, self.auth)
        self.downloads.append(download)
        return True

    def registerFromScript(self, scriptInfo: dict) -> bool:
        try:
            download = _ScriptDownload(self.baseDir, self.workingDir, scriptInfo)
        except AttributeError as e:
            logging.error(f"Invalid download script configuration: {e}")
            return False
        
        self.downloads.append(download)
        return True
