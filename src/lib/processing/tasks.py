from pathlib import Path
import logging
from lib.processing.files import DataFile, StackedFile
from lib.processing.scripts import OutputScript
import lib.downloading as dl
from lib.crawler import Crawler
from lib.converting import Converter
from datetime import date
import lib.processing.parsing as parse
from lib.secrets import Secrets

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
    _auth = "auth"

    def __init__(self, workingDir: Path, config: dict, secretLocation: str):
        self.workingDir = workingDir
        self.auth = None

        self.url = config.get(self._url, None)
        if self.url is None:
            raise Exception("No url provided for source") from AttributeError

        fileName = config.get(self._name, None)
        if fileName is None:
            raise Exception("No filename provided to download to") from AttributeError
        
        auth = config.get(self._auth, False)
        if auth:
            secrets = Secrets(secretLocation)
            self.auth = secrets.getAuth()
    
        properties = config.get(self._properties, {})
        self.file = DataFile(self.workingDir / fileName, properties)

        super().__init__([self.file])

    def run(self, overwrite: bool, verbose: bool) -> bool:
        if not overwrite and self.file.exists():
            logging.info(f"Output file {self.file.path} already exists")
            return False
        
        self.file.delete()
        return dl.download(self.url, self.file.path, verbose=verbose, auth=self.auth)

class CrawlRetrieve(Task):

    _url = Crawler._metaSettingURL
    _regex = Crawler._metaSettingRegex
    _maxDepth = Crawler._metaSettingDepth
    _skipFolders = Crawler._metaSkipFolders

    _link = "link"
    _properties = "properties"
    _filenameURLParts = "urlPrefix"
    _auth = "auth"

    def __init__(self, workingDir: Path, config: dict, secretLocation: str, overwrite: bool):
        self.workingDir = workingDir
        self.auth = None

        url = config.pop(self._url, None)
        regex = config.pop(self._regex, None)
        link = config.pop(self._link, "")
        maxDepth = config.pop(self._maxDepth, -1)
        filenameURLParts = config.pop(self._filenameURLParts, 1)
        skipFolders = config.pop(self._skipFolders, [])
        properties = config.pop(self._properties, {})
        auth = config.pop(self._auth, False)

        if auth:
            secrets = Secrets(secretLocation)
            self.auth = secrets.getAuth()

        crawler = Crawler(self.workingDir, self.auth)
        crawler.run(url, regex, maxDepth, skipFolders, overwrite)
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
            dl.download(downloadURL, downloadFile.path, auth=self.auth, verbose=verbose)
            downloadsRun = True

        return downloadsRun

class ScriptRunner(Task):

    _parallel = "parallel"
    _modulePath = "path"
    _functionName = "function"
    _inputs = "inputs"
    _args = "args"
    _kwargs = "kwargs"
    _outputs = "outputs"

    def __init__(self, workingDir: Path, config: dict, dirLookup: dict[str, Path], downloaded: list[list[DataFile]], processed: list[list[DataFile]]):
        modulePath = config.pop(self._modulePath, "")
        if not modulePath:
            raise Exception("No `path` specified in script config") from AttributeError

        self.modulePath = parse.parsePath(modulePath, workingDir, dirLookup)

        self.functionName = config.pop(self._functionName, "")
        if not self.functionName:
            raise Exception("No `function` specified in script config") from AttributeError

        inputs = config.pop(self._inputs, [])
        self.inputs = parse.parseInputList(inputs, downloaded, processed)

        self.args = config.get(self._args, [])
        self.kwargs = config.get(self._kwargs, {})
        self.parallel = config.pop(self._parallel, False)

    def run(self, overwrite: bool, verbose: bool, lastOutputs: list[Path] = []) -> bool:
        if not self.parallel:
            script = OutputScript(self.modulePath, self.functionName, lastOutputs, self.inputs)
            success, _ = script.run(overwrite, verbose, self.args, self.kwargs)
            return success

        allSuccess = True
        for input, lastOutput in zip(self.inputs, lastOutputs):
            script = OutputScript(self.modulePath, self.functionName, lastOutput, input)
            success, _ = script.run(overwrite, verbose, self.args, self.kwargs)
            allSuccess &= success

        return allSuccess

class Conversion(Task):

    _datasetID = "datasetID"
    _mapID = "mapID"
    _mapColumnName = "mapColumnName"
    _entityEvent = "entityEvent"
    _entityColumn = "entityColumn"
    _chunkSize = "chunkSize"

    def __init__(self, workingDir: Path, mapDir: Path, config: dict, inputFile: DataFile, prefix: str, name: str, subsection: str, retrieveMap: bool):
        self.workingDir = workingDir

        datasetID = config.pop(self._datasetID, "")
        if isinstance(datasetID, dict):
            datasetID = datasetID.get(subsection, "")

        if not datasetID:
            error = "No `datasetID` specified"
            if subsection:
                error += f" for subsection `{subsection}`"

            raise Exception(error) from AttributeError

        mapID = config.pop(self._mapID, "")
        mapColumnName = config.pop(self._mapColumnName, "")
        if not mapID and not mapColumnName:
            raise Exception(f"No `mapID` or `mapColumnName` specified") from AttributeError

        entityEvent = config.pop(self._entityEvent, "collection")
        entityColumn = config.pop(self._entityColumn, "scientific_name")

        outputFile = StackedFile(self.workingDir / name)

        chunkSize = config.pop(self._chunkSize, 1024)

        self.converter = Converter(mapDir, inputFile, outputFile, prefix, datasetID, (entityEvent, entityColumn), chunkSize)
        self.converter.loadMap(mapID, mapColumnName, retrieveMap)

        super().__init__([outputFile])

    def run(self, overwrite: bool, verbose: bool) -> bool:
        success, metadata = self.converter.convert(overwrite, verbose)
        self.setAdditionalMetadata(metadata)
        return success
