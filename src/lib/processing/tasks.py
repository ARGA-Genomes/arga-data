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
import os

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

    _fileSize = "size"
    _fileAccessTime = "atime",
    _fileModTime = "mtime"
    _fileCTime = "ctime"

    def __init__(self, workingDir: Path, foldersAsOutputs: bool = False):
        self.workingDir = workingDir
        self.foldersAsOutputs = foldersAsOutputs
        self._subTasks: list['Task'] = []

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        return True, {}

    def _getWorkingDirFiles(self) -> dict[str, dict[str, int]]:
        files = {}
        for item in self.workingDir.iterdir():
            if self.foldersAsOutputs == item.is_file():
                continue

            files[item.name] = {
                self._fileSize: os.stat(item).st_size,
                self._fileAccessTime: os.stat(item).st_atime_ns,
                self._fileModTime: os.stat(item).st_mtime_ns,
                self._fileCTime: os.stat(item).st_ctime_ns
            }

        return files

    def run(self, overwrite: bool, verbose: bool) -> dict:
        startTime = time.perf_counter()
        startDate = datetime.now().isoformat()

        beforeFiles = self._getWorkingDirFiles()

        try:
            success, extraMetadata = self._execute(overwrite, verbose)
        except KeyboardInterrupt:
            logging.info("Cancelling task execution early")
            return {}
        
        outputs = [name for name, stats in self._getWorkingDirFiles().items() if beforeFiles.get(name, {}) != stats]

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
            if not outputs:
                logging.error("No outputs were created")
                success = False
            else:
                logging.info(f"Created outputs: {', '.join(outputs)}")

            if success:
                metadata |= {
                    Metadata.LAST_SUCCESS_START: startDate,
                    Metadata.LAST_SUCCESS_END: endDate,
                    Metadata.LAST_SUCCESS_DURATION: duration
                }

            return metadata
        
        # Subtasks were generated during execution, run those now
        logging.info(f"Main task generated {len(self._subTasks)} sub-tasks, running those now...")

        metadata[Metadata.TASK_COMPONENTS] = []
        for subTask in self._subTasks:
            subTaskMetadata = subTask.run(overwrite, verbose)

            if not subTaskMetadata:
                metadata[Metadata.SUCCESS] = False
                return metadata

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
    _auth = "auth"

    def __init__(self, workingDir: Path, config: dict, secretLocation: str):
        super().__init__(workingDir)

        self.url = config.get(self._url, None)
        if self.url is None:
            raise Exception("No url provided for source") from AttributeError

        self.fileName = config.get(self._name, None)
        if self.fileName is None:
            raise Exception("No filename provided to download to") from AttributeError

        self.auth = config.get(self._auth, False) # True/False flag
        self.secretLocation = secretLocation

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        auth = None
        if self.auth:
            secrets = Secrets(self.secretLocation)
            auth = secrets.getAuth()
    
        return dl.download(self.url, self.workingDir / self.fileName, verbose=verbose, auth=auth), {}

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

        self.url = config.get(self._url, None)
        self.regex = config.get(self._regex, None)
        self.link = config.get(self._link, "")
        self.maxDepth = config.get(self._maxDepth, -1)
        self.filenameURLParts = config.get(self._filenameURLParts, 1)
        self.skipFolders = config.get(self._skipFolders, [])
        
        self.auth = config.get(self._auth, False) # True/False flag
        self.secretLocation = secretLocation

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        crawler = Crawler(self.workingDir, self.workingDir.parent, self.auth)
        crawler.run(self.url, self.regex, self.maxDepth, self.skipFolders, overwrite)
        urlList = crawler.getFileURLs(self.link)

        for url in urlList:
            fileName = "_".join(url.split("/")[-self.filenameURLParts:])
            downloadConfig = {
                UrlRetrieve._url: url,
                UrlRetrieve._name: fileName,
                UrlRetrieve._auth: self.auth
            }

            self._subTasks.append(UrlRetrieve(self.workingDir, downloadConfig, self.secretLocation))

        return True, {}

class ScriptRunner(Task):

    _parallel = "parallel"
    _modulePath = "path"
    _functionName = "function"
    _inputs = "inputs"
    _args = "args"
    _kwargs = "kwargs"

    def __init__(self, workingDir: Path, config: dict, dirLookup: dict[str, Path], downloaded: list[list[DataFile]], processed: list[list[DataFile]], _parseConfig: bool = True):
        super().__init__(workingDir)

        modulePath = config.get(self._modulePath, "")
        if not modulePath:
            raise Exception("No `path` specified in script config") from AttributeError

        self.modulePath = parse.parsePath(modulePath, workingDir, dirLookup) if _parseConfig else modulePath

        self.functionName = config.get(self._functionName, "")
        if not self.functionName:
            raise Exception("No `function` specified in script config") from AttributeError

        inputs = config.get(self._inputs, [])
        self.inputs = parse.parseInputList(inputs, downloaded, processed) if _parseConfig else inputs

        self.args = config.get(self._args, [])
        self.kwargs = config.get(self._kwargs, {})
        self.parallel = config.get(self._parallel, False)

        self._dirLookup = dirLookup
        self._downloaded = downloaded
        self._processed = processed

    def _execute(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        if not self.parallel:
            script = OutputScript(self.modulePath, self.functionName, self.workingDir, self.inputs)
            success, _ = script.run(verbose, self.args, self.kwargs)
            return success, {}

        for input in self.inputs:
            scriptConfig = {
                ScriptRunner._modulePath: self.modulePath,
                ScriptRunner._functionName: self.functionName,
                ScriptRunner._inputs: self.inputs,
                ScriptRunner._args: self.args,
                ScriptRunner._kwargs: self._kwargs
            }

            self._subTasks.append(ScriptRunner(self.workingDir, scriptConfig, self._dirLookup, self._downloaded, self._processed, False))

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

        self.datasetID = config.get(self._datasetID, "")
        if not self.datasetID:
            raise Exception("No `datasetID` specified") from AttributeError

        self.mapID = config.get(self._mapID, "")
        self.mapColumnName = config.get(self._mapColumnName, "")
        if not self.mapID and not self.mapColumnName:
            raise Exception(f"No `mapID` or `mapColumnName` specified") from AttributeError
        
        self.input = config.get(self._input, "")
        if not self.input:
            raise Exception(f"No `input` specified") from AttributeError
        
        self.input = parse.parseInput(self.input, downloaded, processed)[0] # Singular input

        self.entityEvent = config.get(self._entityEvent, "collection")
        self.entityColumn = config.get(self._entityColumn, "scientific_name")
        self.chunkSize = config.get(self._chunkSize, 1024)

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
