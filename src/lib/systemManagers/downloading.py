from pathlib import Path
from lib.systemManagers.baseManager import SystemManager, Task
from lib.processing.files import DataFile, Step
from lib.processing.scripts import OutputScript
import logging
import lib.downloading as dl

class _Download(Task):
    def __init__(self, filePath: Path, properties: dict):
        super().__init__()
        
        self.file = DataFile(filePath, properties)

    def getOutputPath(self) -> Path:
        return self.file.path

class _URLDownload(_Download):
    def __init__(self, url: str, filePath: Path, properties: dict, auth: dl.HTTPBasicAuth):
        self.url = url
        self.auth = auth

        super().__init__(filePath, properties)

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        if not overwrite and self.file.exists():
            logging.info(f"Output file {self.file.path} already exists")
            return False
        
        self.file.delete()
        return dl.download(self.url, self.file.path, verbose=verbose, auth=self.auth)

class _ScriptDownload(_Download):
    def __init__(self, scriptDir: Path, downloadDir: Path, scriptInfo: dict, imports: dict[str, Path]):
        self.script = OutputScript(scriptDir, dict(scriptInfo), downloadDir, imports)

        super().__init__(self.script.output.path, self.script.output.properties)

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        return self.script.run(overwrite, verbose)[0] # No retval for downloading tasks, just return success

class DownloadManager(SystemManager):
    def __init__(self, dataDir: Path, scriptDir: Path, metadataDir: Path, scriptImports: dict[str, Path], username: str, password: str):
        super().__init__(dataDir, scriptDir, metadataDir, Step.DOWNLOADING, "files")

        self.scriptImports = scriptImports
        self.auth = dl.buildAuth(username, password) if username else None

    def getFiles(self) -> list[DataFile]:
        return [download.file for download in self._tasks]

    def download(self, overwrite: bool = False, verbose: bool = False) -> bool:
        return self.runTasks(overwrite, verbose)

    def registerFromURL(self, url: str, fileName: str, fileProperties: dict = {}) -> bool:
        download = _URLDownload(url, self.workingDir / fileName, fileProperties, self.auth)
        self._tasks.append(download)
        return True

    def registerFromScript(self, scriptInfo: dict) -> bool:
        try:
            download = _ScriptDownload(self.scriptDir, self.workingDir, scriptInfo, self.scriptImports)
        except AttributeError as e:
            logging.error(f"Invalid download script configuration: {e}")
            return False
        
        self._tasks.append(download)
        return True
