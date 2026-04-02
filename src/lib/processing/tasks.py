from pathlib import Path
import logging
from lib.processing.files import DataFile, StackedFile
from lib.processing.scripts import OutputScript
import lib.downloading as dl
from lib.crawler import Crawler
from lib.converting import Converter
import lib.processing.parsing as parse
from lib.secrets import Secrets

class Task:
    def __init__(self, workingDir: Path):
        self.workingDir = workingDir

    def _execute(self, overwrite: bool, verbose: bool):
        return True

    def run(self, overwrite: bool, verbose: bool, lastOutputs: list[DataFile] = []) -> bool:
        return self._execute(overwrite, verbose, lastOutputs)            

class UrlRetrieve(Task):

    _url = "url"
    _name = "name"
    _properties = "properties"
    _auth = "auth"

    def __init__(self, workingDir: Path, config: dict, secretLocation: str):
        super().__init__(workingDir)

        self.url = config.get(self._url, None)
        if self.url is None:
            raise Exception("No url provided for source") from AttributeError

        self.fileName = config.get(self._name, None)
        if self.fileName is None:
            raise Exception("No filename provided to download to") from AttributeError

        self.auth # Default value
        auth = config.get(self._auth, False) # True/False flag
        if auth:
            secrets = Secrets(secretLocation)
            self.auth = secrets.getAuth()

    def run(self, overwrite: bool, verbose: bool) -> bool:
        return dl.download(self.url, self.workingDir / self.fileName, verbose=verbose, auth=self.auth)

class CrawlRetrieve(Task):

    _url = Crawler._metaSettingURL
    _regex = Crawler._metaSettingRegex
    _maxDepth = Crawler._metaSettingDepth
    _skipFolders = Crawler._metaSkipFolders

    _link = "link"
    _properties = "properties"
    _filenameURLParts = "urlPrefix"
    _auth = "auth"

    def __init__(self, workingDir: Path, config: dict, secretLocation: str):
        super().__init__(workingDir)

        self.url = config.pop(self._url, None)
        self.regex = config.pop(self._regex, None)
        self.link = config.pop(self._link, "")
        self.maxDepth = config.pop(self._maxDepth, -1)
        self.filenameURLParts = config.pop(self._filenameURLParts, 1)
        self.skipFolders = config.pop(self._skipFolders, [])
        
        self.auth = None
        auth = config.pop(self._auth, False)
        if auth:
            secrets = Secrets(secretLocation)
            self.auth = secrets.getAuth()

    def run(self, overwrite: bool, verbose: bool) -> bool:
        crawler = Crawler(self.workingDir, self.auth)
        crawler.run(self.url, self.regex, self.maxDepth, self.skipFolders, overwrite)
        urlList = crawler.getFileURLs(self.link)

        allSuccess = True
        for url in urlList:
            fileName = "_".join(url.split("/")[-self.filenameURLParts:])
            success = dl.download(url, self.workingDir / fileName, auth=self.auth, verbose=verbose)
            allSuccess &= success

        return allSuccess

class ScriptRunner(Task):

    _parallel = "parallel"
    _modulePath = "path"
    _functionName = "function"
    _inputs = "inputs"
    _args = "args"
    _kwargs = "kwargs"
    _outputs = "outputs"

    def __init__(self, workingDir: Path, config: dict, dirLookup: dict[str, Path], downloaded: list[list[DataFile]], processed: list[list[DataFile]]):
        super().__init__(workingDir)

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
    _input = "input"
    _entityEvent = "entityEvent"
    _entityColumn = "entityColumn"
    _chunkSize = "chunkSize"

    def __init__(self, workingDir: Path, mapDir: Path, config: dict, prefix: str, name: str, subsection: str, downloaded: list[list[DataFile]], processed: list[list[DataFile]]):
        super().__init__(workingDir)

        self.datasetID = config.pop(self._datasetID, "")
        if isinstance(self.datasetID, dict):
            self.datasetID = self.datasetID.get(subsection, "")

        if not self.datasetID:
            error = "No `datasetID` specified"
            if subsection:
                error += f" for subsection `{subsection}`"

            raise Exception(error) from AttributeError

        self.mapID = config.pop(self._mapID, "")
        self.mapColumnName = config.pop(self._mapColumnName, "")
        if not self.mapID and not self.mapColumnName:
            raise Exception(f"No `mapID` or `mapColumnName` specified") from AttributeError
        
        self.input = config.pop(self._input, "")
        if not self.input:
            raise Exception(f"No `input` specified") from AttributeError
        
        self.input = parse.parseInput(self.input, downloaded, processed)[0] # Singular input

        self.entityEvent = config.pop(self._entityEvent, "collection")
        self.entityColumn = config.pop(self._entityColumn, "scientific_name")
        self.chunkSize = config.pop(self._chunkSize, 1024)

        self.mapDir = mapDir
        self.prefix = prefix
        self.name = name
        self.subsection = subsection

    def run(self, overwrite: bool, verbose: bool) -> bool:
        converter = Converter(self.mapDir, self.input, self.workingDir / self.name, self.prefix, self.datasetID, (self.entityEvent, self.entityColumn), self.chunkSize)
        converter.loadMap(self.mapID, self.mapColumnName, overwrite)
        success, metadata = converter.convert(overwrite, verbose)
        return success
