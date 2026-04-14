from pathlib import Path
import logging
from lib.processing.files import DataFile
from lib.processing.scripts import OutputScript
import lib.downloading as dl
from lib.crawler import Crawler
from lib.converting import Converter
import lib.processing.parsing as parse
from lib.secrets import Secrets
import time
from datetime import datetime
from enum import Enum
from lib.processing.mapping import Map

class Metadata(Enum):
    OUTPUTS = "outputs"
    SUCCESS = "success"
    TASK_START = "task started"
    TASK_END = "task completed"
    TASK_DURATION = "duration"
    LAST_SUCCESS_START = "last success started"
    LAST_SUCCESS_END = "last success completed"
    LAST_SUCCESS_DURATION = "last success duration"
    TOTAL_DURATION = "total duration"
    LAST_SUCCESS_TOTAL_DURATION = "last success total duration"
    TASK_COMPONENTS = "task components"
    CUSTOM = ""

class Task:
    def __init__(self, workingDir: Path, foldersAsOutputs: bool = False):
        self.workingDir = workingDir
        self.foldersAsOutputs = foldersAsOutputs
        self._subTasks: list['Task'] = []

    @staticmethod
    def fromVars(cls: 'Task', workingDir: Path, **kwargs: dict):
        oldInit = cls.__init__
        cls.__init__ = lambda *args, **kwargs: None

        instance = cls()
        super(instance).__init__(workingDir)

        cls.__init__ = oldInit
        for key, value in kwargs.items():
            setattr(cls, key, value)

        return instance
    
    def addSubTask(self, subTask: 'Task') -> None:
        self._subTasks.append(subTask)

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        return True, {}

    def run(self, overwrite: bool, verbose: bool) -> dict:
        startTime = time.perf_counter()
        startDate = datetime.now().isoformat()

        workingDirFiles = [item for item in self.workingDir.iterdir()]

        try:
            success, extraMetadata = self._execute(overwrite, verbose)
        except KeyboardInterrupt:
            logging.info("Cancelling task execution early")
            return
        
        outputs = [item.name for item in self.workingDir.iterdir() if item not in workingDirFiles and (item.is_file() if not self.foldersAsOutputs else item.is_dir())]

        duration = time.perf_counter() - startTime
        endDate = datetime.now().isoformat()

        metadata = {
            Metadata.OUTPUTS: outputs,
            Metadata.SUCCESS: success,
            Metadata.TASK_START: startDate,
            Metadata.TASK_DURATION: duration,
            Metadata.TASK_END: endDate,
            Metadata.CUSTOM: extraMetadata
        }

        if not self._subTasks:
            if success:
                metadata |= {
                    Metadata.LAST_SUCCESS_START: startDate,
                    Metadata.LAST_SUCCESS_END: endDate,
                    Metadata.LAST_SUCCESS_DURATION: duration
                }

            return metadata
        
        # Subtasks were generated during execution, run those now
        metadata[Metadata.TASK_COMPONENTS] = []
        for subTask in self._subTasks:
            subTaskMetadata = subTask.run(overwrite, verbose)

            metadata[Metadata.OUTPUTS] = metadata[Metadata.OUTPUTS] + subTaskMetadata[Metadata.OUTPUTS]
            metadata[Metadata.SUCCESS] = metadata[Metadata.SUCCESS] & subTaskMetadata[Metadata.SUCCESS]
            metadata[Metadata.TASK_COMPONENTS].append(subTaskMetadata)

        duration = time.perf_counter() - startTime
        endDate = datetime.now().isoformat()

        metadata[Metadata.TASK_DURATION] = duration
        metadata[Metadata.TASK_END] = endDate

        if metadata[Metadata.SUCCESS]:
            metadata |= {
                Metadata.LAST_SUCCESS_START: startDate,
                Metadata.LAST_SUCCESS_END: endDate,
                Metadata.LAST_SUCCESS_DURATION: duration
            }

        return metadata

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

        self.auth = None # Default value
        auth = config.get(self._auth, False) # True/False flag
        if auth:
            secrets = Secrets(secretLocation)
            self.auth = secrets.getAuth()

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        return dl.download(self.url, self.workingDir / self.fileName, verbose=verbose, auth=self.auth), {}

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

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        crawler = Crawler(self.workingDir, self.workingDir.parent, self.auth)
        crawler.run(self.url, self.regex, self.maxDepth, self.skipFolders, overwrite)
        urlList = crawler.getFileURLs(self.link)

        for url in urlList:
            fileName = "_".join(url.split("/")[-self.filenameURLParts:])
            self.addSubTask(UrlRetrieve.fromVars(self.workingDir, {"url": url, "fileName": fileName, "auth": self.auth}))

        return True, {}

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

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        if not self.parallel:
            script = OutputScript(self.modulePath, self.functionName, self.workingDir, self.inputs)
            success, _ = script.run(verbose, self.args, self.kwargs)
            return success, {}

        for input in self.inputs:
            self.addSubTask(ScriptRunner.fromVars(self.workingDir, {"modulePath": self.modulePath, "functionName": self.functionName, "inputs": [input], "args": self.args, "kwargs": self.kwargs, "parallel": False}))

        return True, {}

class Conversion(Task):

    _datasetID = "datasetID"
    _mapID = "mapID"
    _mapColumnName = "mapColumnName"
    _input = "input"
    _entityEvent = "entityEvent"
    _entityColumn = "entityColumn"
    _chunkSize = "chunkSize"

    _localMapName = "map.json"

    def __init__(self, workingDir: Path, config: dict, name: str, dataDate: str, unmappedPrefix: str, downloaded: list[list[DataFile]], processed: list[list[DataFile]]):
        super().__init__(workingDir, True)

        self.datasetID = config.pop(self._datasetID, "")
        if not self.datasetID:
            raise Exception("No `datasetID` specified") from AttributeError

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

        self.unmappedPrefix = unmappedPrefix
        self.fileName = f"{name}_{dataDate}"

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        mapDir = self.workingDir.parent
        localMapFile = mapDir / self._localMapName

        if not localMapFile.exists() or overwrite:
            if self.mapColumnName:
                logging.info("Using updated mapping sheet")
                map = Map.fromModernSheet(self.mapColumnName, localMapFile)
            elif self.mapID:
                logging.info("Using original mapping sheet")
                map = Map.fromSheets(self.mapID, localMapFile)
            else: # Should never land here as mapID and mapColumnName are verified to exist in init
                logging.warning("No mapping found")
                return False, {}
        else:
            logging.info(f"Using local map file {localMapFile}")
            map = Map.fromFile(localMapFile, self.unmappedPrefix)

        converter = Converter(self.input, self.workingDir / self.fileName)
        return converter.convert(map, self.chunkSize, self.datasetID, self.entityEvent, self.entityColumn, verbose)
