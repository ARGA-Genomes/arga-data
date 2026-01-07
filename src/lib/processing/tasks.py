from pathlib import Path
import logging
from lib.processing.files import DataFile, StackedFile
from lib.processing.scripts import OutputScript
import lib.downloading as dl
from lib.crawler import Crawler
from lib.converting import Converter
from datetime import date
from lib.settings import globalSettings as gs

class Task:
    def __init__(self, outputs: list[DataFile] = []):
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

class UrlRetrieve(Task):

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

    def run(self, overwrite: bool, verbose: bool) -> bool:
        if not overwrite and self.file.exists():
            logging.info(f"Output file {self.file.path} already exists")
            return False
        
        self.file.delete()
        return dl.download(self.url, self.file.path, verbose=verbose, auth=dl.buildAuth(self.username, self.password))

class CrawlRetrieve(Task):

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

    def run(self, overwrite: bool, verbose: bool) -> bool:
        downloadsRun = False
        for downloadURL, downloadFile in self.downloads:
            if not overwrite and downloadFile.exists():
                continue

            downloadFile.delete()
            dl.download(downloadURL, downloadFile.path, auth=dl.buildAuth(self.username, self.password), verbose=verbose)
            downloadsRun = True

        return downloadsRun

class ScriptRunner(Task):

    _modulePath = "path"
    _functionName = "function"
    _inputs = "inputs"
    _args = "args"
    _kwargs = "kwargs"
    _outputs = "outputs"

    def __init__(self, workingDir: Path, config: dict, libraryDirs: list[Path]):
        modulePath = config.pop(self._modulePath, "")
        if not modulePath:
            raise Exception("No `path` specified in script config") from AttributeError

        functionName = config.pop(self._functionName, "")
        if not functionName:
            raise Exception("No `function` specified in script config") from AttributeError

        outputs = config.pop(self._outputs, [])
        if not outputs:
            raise Exception("No `outputs` specified in script config") from AttributeError        

        outputs = [DataFile(workingDir / output) for output in outputs]
        inputs = [DataFile(input) for input in config.pop(self._inputs, [])]

        self.args = config.pop(self._args, [])
        self.kwargs = config.pop(self._kwargs, {})

        self.script = OutputScript(modulePath, functionName, outputs, inputs, libraryDirs)

        super().__init__(outputs)

    def run(self, overwrite: bool, verbose: bool) -> bool:
        success, _ = self.script.run(overwrite, verbose, self.args, self.kwargs)
        return success

class Conversion(Task):

    _mapID = "mapID"
    _mapColumnName = "mapColumnName"
    _entityEvent = "entityEvent"
    _entityColumn = "entityColumn"
    _timestamp = "timestamp"
    _chunkSize = "chunkSize"

    def __init__(self, workingDir: Path, config: dict, datasetID: str, prefix: str, name: str, libraryDirs: dict):
        self.workingDir = workingDir
        self.datasetID = datasetID

        mapID = config.pop(self._mapID, "")
        mapColumnName = config.pop(self._mapColumnName, "")
        if not mapID and not mapColumnName:
            raise Exception(f"No `mapID` or `mapColumnName` specified") from AttributeError

        entityEvent = config.pop(self._entityEvent, "collection")
        entityColumn = config.pop(self._entityColumn, "scientific_name")

        timeStamp = config.pop(self._timestamp, True)
        outputFile = StackedFile(self.workingDir / f"{name}{date.today().strftime('-%Y-%m-%d') if timeStamp else ''}")

        chunkSize = config.pop(self._chunkSize, 1024)

        self.converter = Converter(outputFile, prefix, (entityEvent, entityColumn), chunkSize)
        
        super().__init__([outputFile])

    def run(self, overwrite: bool, verbose: bool) -> bool:
        if self.datasetID is None:
            logging.error("No datasetID provided which is required for conversion, exiting...")
            return False

