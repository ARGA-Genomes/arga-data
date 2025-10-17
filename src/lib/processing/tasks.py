from pathlib import Path
import logging
from lib.processing.files import DataFile
from lib.processing.scripts import OutputScript, FileScript
import lib.downloading as dl
from lib.crawler import Crawler

class Task:
    def __init__(self, outputs: list[DataFile]):
        self._runMetadata = {}
        self._outputs = outputs

    def getOutputs(self) -> list[DataFile]:
        return self._outputs

    def run(self, overwrite: bool, verbose: bool) -> bool:
        return True
    
    def setAdditionalMetadata(self, metadata: dict) -> None:
        self._runMetadata.update(metadata)

    def getMetadata(self) -> dict:
        return self._runMetadata

class URLDownload(Task):

    _url = "url"
    _name = "name"
    _properties = "properties"

    def __init__(self, workingDir: Path, username: str, password: str, config: dict):
        self.workingDir = workingDir
        self.username = username
        self.password = password

        self.url = config.get(self.url, None)
        if self.url is None:
            raise Exception("No url provided for source") from AttributeError

        fileName = config.get(self._name, None)
        if fileName is None:
            raise Exception("No filename provided to download to") from AttributeError
    
        properties = config.get(self._properties, {})
        self.file = DataFile(self.workingDir / fileName, properties)

        super().__init__([self.file])

    def getOutputs(self) -> list[DataFile]:
        return [self.file]

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        if not overwrite and self.file.exists():
            logging.info(f"Output file {self.file.path} already exists")
            return False
        
        self.file.delete()
        return dl.download(self.url, self.file.path, verbose=verbose, auth=dl.buildAuth(self.username, self.password))

class CrawlDownload(Task):

    _url = "url"
    _regex = "regex"
    _link = "link"
    _maxDepth = "maxDepth"
    _properties = "properties"
    _filenameURLParts = "urlPrefix"

    def __init__(self, workingDir: Path, username: str, password: str, config: dict, overwrite: bool):
        self.workingDir = workingDir
        self.username = username
        self.password = password

        url = config.pop(self._url, None)
        regex = config.pop(self._regex, None)
        link = config.pop(self._link, "")
        maxDepth = config.pop(self._maxDepth, -1)
        filenameURLParts = config.pop(self._filenameURLParts, 1)
        properties = config.pop(self._properties, {})

        crawler = Crawler(self.workingDir, dl.buildAuth(self.username, self.password))
        crawler.run(url, regex, maxDepth, overwrite)
        urlList = crawler.getFileURLs(link)

        self.downloads: list[tuple[str, DataFile]] = []
        for url in urlList:
            fileName = "_".join(url.split("/")[-filenameURLParts:])
            self.downloads.append((url, DataFile(self.workingDir / fileName, properties)))

        super().__init__([downloadFile for _, downloadFile in self.downloads])

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        downloadsRun = False
        for downloadURL, downloadFile in self.downloads:
            if not overwrite and downloadFile.exists():
                continue

            downloadFile.delete()
            dl.download(downloadURL, downloadFile.path, auth=dl.buildAuth(self.username, self.password), verbose=verbose)
            downloadsRun = True

        return downloadsRun

class ScriptDownload(Task):

    _libraryLink = ".llib"

    def __init__(self, scriptDir: Path, workingDir: Path, libraryDir: Path, config: dict):
        self.script = OutputScript(scriptDir, dict(config), workingDir, [libraryDir])

        super().__init__(self.script.outputs)

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        return self.script.run(overwrite, verbose)[0] # No retval for downloading tasks, just return success
    
class ProcessingNode(Task):
    def __init__(self, scriptDir: Path, workingDir: Path, libraryDir: Path, inputs: list[DataFile], config: dict):
        self.script = FileScript(scriptDir, dict(config), workingDir, ..., [libraryDir])

        super().__init__(self.script.outputs)

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        return self.script.run(overwrite, verbose)[0] # No retval for processing tasks, just return success
