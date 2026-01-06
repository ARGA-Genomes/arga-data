import json
import logging
from lib.settings import globalSettings as gs
from lib.secrets import secrets
from enum import Enum
from pathlib import Path
from lib.processing import tasks
import lib.processing.updating as upd
from typing import Any
from lib.processing.files import DataFile
import time
from datetime import datetime

sourceConfigName = "config.json"

class Flag(Enum):
    VERBOSE   = "quiet" # Verbosity enabled by default, flag is used when silenced
    REPREPARE = "reprepare"
    OVERWRITE = "overwrite"

class Step(Enum):
    DOWNLOADING = "downloading"
    PROCESSING  = "processing"

class Retrieve(Enum):
    URL     = "url"
    CRAWL   = "crawl"
    SCRIPT  = "script"

class FileSelect(Enum):
    INPUT    = "IN"
    OUTPUT   = "OUT"
    DOWNLOAD = "D"
    PROCESS  = "P"

class _FileProperty(Enum):
    DIR  = "DIR"
    FILE = "FILE"
    PATH = "PATH"

class Updates(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

class Metadata(Enum):
    OUTPUT = "output"
    SUCCESS = "success"
    TASK_START = "task started"
    TASK_END = "task completed"
    TASK_DURATION = "duration"
    LAST_SUCCESS_START = "last success started"
    LAST_SUCCESS_END = "last success completed"
    LAST_SUCCESS_DURATION = "last success duration"
    TOTAL_DURATION = "total duration"
    LAST_SUCCESS_TOTAL_DURATION = "last success total duration"
    CUSTOM = ""

class Database:
    _metadataFileName = "metadata.json"

    def __init__(self, databaseDir: Path):
        self.databaseDir = databaseDir

        configPath = databaseDir / sourceConfigName
        if configPath.exists():
            with open(configPath) as fp:
                config = json.load(fp)
        else:
            config = {}

        self.subsections: dict[str, dict[str, str]] = config.pop("subsections", {})
        self.config = config

    def shortName(self) -> str:
        return self.databaseDir.name

    def listSubsections(self) -> list[str]:
        return list(self.subsections)

    def constuct(self, name: str, subsection: str):
        self.name = name
        self.subsectionDir = self.databaseDir / subsection # Same as databaseDir if no subsection
        self.locationDir = self.databaseDir.parent

        # Subsection remapping
        if subsection:
            subsectionTags = self.subsections.get(subsection, {}).get("tags", {})
            rawConfig = json.dumps(self.config)
            rawConfig = rawConfig.replace("<SUB>", subsection)

            for tag, replaceValue in subsectionTags.items():
                rawConfig = rawConfig.replace(f"<SUB:{tag.upper()}>", replaceValue)

            self.config = json.loads(rawConfig)

        # Local storage and libraries
        self.exampleDir = self.subsectionDir / "examples" # Data sample storage location
        locationLib = self.locationDir / "llib" # Location based library for scripts shared across a location
        scriptsLib = self.databaseDir / "scripts" # Database specific scripts
        self.libDirs = [locationLib, scriptsLib]

        # Local settings
        self.settings = gs
        for dir in (self.locationDir, self.databaseDir, self.subsectionDir):
            subdirConfig = Path(dir / "settings.toml")
            if subdirConfig.exists():
                self.settings = self.settings.createChild(subdirConfig)

        # Data storage
        self.dataDir = self.subsectionDir / "data" # Default data location
        if self.settings.storage.data: # Overwrite data location
            self.dataDir: Path = self.settings.storage.data / self.dataDir.relative_to(self.locationDir.parent) # Change parent of locationDir (dataSources folder) to storage dir

        self.downloadDir = self.dataDir / Step.DOWNLOADING.value
        self.processingDir = self.dataDir / Step.PROCESSING.value

        # Tasks
        self._queuedTasks: dict[Step, list] = {Step.DOWNLOADING: [], Step.PROCESSING: [[]]}
        self._metadataPath = self.subsectionDir / self._metadataFileName
        self._metadata = self._loadMetadata()

        self._dirLookup = {f".{directory.name}": directory for directory in (self.libDirs + [self.downloadDir, self.processingDir])}

        # Updating
        updateConfig: dict = self.config.pop("updating", {})
        self._update = self._getUpdater(updateConfig)

        # Preparation Stage
        self._prepStage = -1

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)
    
    def _reportLeftovers(self, config: dict, sectionName: str) -> None:
        for property in config:
            logging.debug(f"{self.name} unknown{f' {sectionName}' if sectionName else ''} config item: {property}")

    def _prepareDownload(self, flags: list[Flag]) -> None:
        downloadConfig: dict = self.config.pop(Step.DOWNLOADING.value, {})
        if not downloadConfig:
            raise Exception(f"No download config specified as required for {self.name}") from AttributeError

        retrieveType = downloadConfig.pop("retrieveType", None)
        if retrieveType is None:
            raise Exception(f"No retrieve type specified in download config for {self.name}") from AttributeError
        
        downloadTaskConfig = downloadConfig.pop("tasks", None)
        if downloadTaskConfig is None:
            raise Exception(f"No download tasks specified in download config for {self.name}") from AttributeError

        # Get username/password for url/crawl downloads
        sourceSecrets = secrets[self.locationDir.name]
        username = sourceSecrets.username if sourceSecrets is not None else ""
        password = sourceSecrets.password if sourceSecrets is not None else ""

        overwrite = Flag.REPREPARE in flags

        retrieve = Retrieve._value2member_map_.get(retrieveType, None)
        if retrieve == Retrieve.URL: # Tasks should be a list of dicts
            if not isinstance(downloadTaskConfig, list):
                raise Exception(f"URL retrieve type expects a list of task configs for {self.name}") from AttributeError

            for taskConfig in downloadTaskConfig:
                parsedConfig = self.parseTaskConfig(taskConfig, self.downloadDir)
                self._queuedTasks[Step.DOWNLOADING].append(tasks.UrlRetrieve(self.downloadDir, username, password, parsedConfig))

        elif retrieve == Retrieve.CRAWL: # Tasks should be dict
            if not isinstance(downloadTaskConfig, dict):
                raise Exception(f"Crawl retrieve type expects a dict config for {self.name}")
            
            parsedConfig = self.parseTaskConfig(downloadTaskConfig, self.downloadDir)
            self._queuedTasks[Step.DOWNLOADING].append(tasks.CrawlRetrieve(self.downloadDir, username, password, parsedConfig, overwrite))

        elif retrieve == Retrieve.SCRIPT: # Tasks should be dict
            if not isinstance(downloadTaskConfig, dict):
                raise Exception(f"Script retrieve type expects a dict config for {self.name}")
            
            parsedConfig = self.parseTaskConfig(downloadTaskConfig, self.downloadDir)
            self._queuedTasks[Step.DOWNLOADING].append(tasks.ScriptRunner(self.downloadDir, parsedConfig, self.libDirs))

        else:
            raise Exception(f"Unknown retrieve type '{retrieveType}' specified for {self.name}") from AttributeError

    def _prepareProcessing(self, flags: list[Flag]) -> None:
        processingConfig: dict = self.config.pop(Step.PROCESSING.value, [])

        for idx, processingStep in enumerate(processingConfig):
            # parsedConfig = self.parseTaskConfig(processConfig, self.processingDir)
            # self._queuedTasks[Step.PROCESSING].append(tasks.ScriptRunner(self.processingDir, parsedConfig, self.libDirs))
            ...

    def _prepare(self, fileStep: Step, flags: list[Flag]) -> bool:
        callbacks = {
            Step.DOWNLOADING: self._prepareDownload,
            Step.PROCESSING: self._prepareProcessing,
        }

        if fileStep not in callbacks:
            raise Exception(f"Unknown step to prepare: {fileStep}")

        for idx, (stepType, callback) in enumerate(callbacks.items()):
            if idx <= self._prepStage:
                continue

            logging.info(f"Preparing {self} step '{stepType.name}' with flags: {self._printFlags(flags)}")
            try:
                callback(flags)
            except AttributeError as e:
                logging.error(f"Error preparing step: {stepType.name} - {e}")
                return False
            
            self._prepStage = idx
            if fileStep == stepType:
                break
            
        return True
    
    def _execute(self, step: Step, flags: list[Flag]) -> bool:
        overwrite = Flag.OVERWRITE in flags
        verbose = Flag.VERBOSE in flags
        logging.info(f"Executing {self} step '{step.name}' with flags: {self._printFlags(flags)}")

        if step == Step.DOWNLOADING:
            tasks = self._queuedTasks[Step.DOWNLOADING]
        elif step == Step.PROCESSING:
            tasks = [task for layer in self._queuedTasks[Step.PROCESSING] for task in layer]

        allSucceeded = False
        startTime = time.perf_counter()
        for idx, task in enumerate(tasks):
            taskStartTime = time.perf_counter()
            taskStartDate = datetime.now().isoformat()

            success = task.run(overwrite, verbose)

            duration = time.perf_counter() - taskStartTime
            taskEndDate = datetime.now().isoformat()

            packet = {
                Metadata.OUTPUT: task.getOutputPath().name,
                Metadata.SUCCESS: success,
                Metadata.TASK_START: taskStartDate,
                Metadata.TASK_END: taskEndDate,
                Metadata.TASK_DURATION: duration,
            }

            # Task can set metadata on itself during run
            extraMetadata = task.getMetadata()
            if extraMetadata:
                packet[Metadata.CUSTOM] = extraMetadata

            if success:
                packet |= {
                    Metadata.LAST_SUCCESS_START: taskStartDate,
                    Metadata.LAST_SUCCESS_END: taskEndDate,
                    Metadata.LAST_SUCCESS_DURATION: duration
                }

            self.updateMetadata(step, idx, packet)
            allSucceeded = allSucceeded and success

        self.updateTotalTime(time.perf_counter() - startTime, allSucceeded)
        return allSucceeded
    
    def create(self, fileStep: Step, flags: list[Flag]) -> None:
        try:
            success = self._prepare(fileStep, flags)
            if not success:
                return
            
        except KeyboardInterrupt:
            logging.info(f"Process ended early when attempting to prepare step '{fileStep.name}' for {self.name}")

        try:
            self._execute(fileStep, flags)
        except KeyboardInterrupt:
            logging.info(f"Process ended early when attempting to execute step '{fileStep.name}' for {self.name}")

    def checkUpdateReady(self) -> bool:
        return self._update.updateReady(self.getLastUpdate(Step.DOWNLOADING))
    
    def update(self, flags: list[Flag]) -> bool:
        for fileStep in (Step.DOWNLOADING, Step.PROCESSING):
            self.create(fileStep, list(set(flags).add(Flag.OVERWRITE)))

    def _printFlags(self, flags: list[Flag]) -> str:
        return " | ".join(f"{flag.value}={flag in flags}" for flag in Flag)
    
    def parseTaskConfig(self, config: dict, relativeDir: Path):
        res = {}

        for key, value in config.items():
            if isinstance(value, list):
                res[key] = [self._parseArg(arg, relativeDir) for arg in value]

            elif isinstance(value, dict):
                res[key] = self.parseTaskConfig(value, relativeDir)

            else:
                res[key] = self._parseArg(value, relativeDir)
    
    def _parseArg(self, arg: Any, parentDir: Path) -> Path | str:
        if not isinstance(arg, str):
            return arg

        if arg.startswith("."):
            return self._parsePath(arg, parentDir)
        
        if arg.startswith("{") and arg.endswith("}"):
            parsedArg = self._parseSelectorArg(arg[1:-1])
            if parsedArg == arg:
                logging.warning(f"Unknown key code: {parsedArg}")

            return parsedArg

        return arg

    def _parsePath(self, arg: str, parentPath: Path) -> Path | Any:
        prefix, relPath = arg.split("/", 1)
        if prefix == ".":
            return parentPath / relPath
            
        if prefix == "..":
            cwd = parentPath.parent
            while relPath.startswith("../"):
                cwd = cwd.parent
                relPath = relPath[3:]

            return cwd / relPath
            
        if prefix in self._dirLookup:
            return self._dirLookup[prefix] / relPath
        
        return arg

    def _parseSelectorArg(self, arg: str) -> Path | str:
        print(arg)
        if "-" not in arg:
            logging.warning(f"Both file type and file property not present in arg, deliminate with '-'")
            return arg
        
        fType, fProperty = arg.split("-")

        if fType[-1].isdigit():
            selection = int(fType[-1])
            fType = fType[:-1]
        else:
            selection = 0

        fTypeEnum = FileSelect._value2member_map_.get(fType, None)
        if fTypeEnum is None:
            logging.error(f"Invalid file type: '{file}'")
            return arg

        files = self.fileLookup.get(fTypeEnum, None)
        if files is None:
            logging.error(f"No files provided for file type: '{fType}")
            return arg

        if selection > len(files):
            logging.error(f"File selection '{selection}' out of range for file type '{fType}' which has a length of '{len(files)}")
            return arg
        
        file: DataFile = files[selection]
        fProperty, *suffixes = fProperty.split(".")

        if fProperty == _FileProperty.FILE.value:
            if suffixes:
                logging.warning("Suffix provided for a file object which cannot be resolved, suffix not applied")
            return file
        
        if fProperty == _FileProperty.DIR.value:
            if suffixes:
                logging.warning("Suffix provided for a parent path which cannot be resolved, suffix not applied")
            return file.path.parent

        if fProperty == _FileProperty.PATH.value:
            pth = file.path
            for suffix in suffixes:
                pth = pth.with_suffix(suffix if not suffix else f".{suffix}") # Prepend a dot for valid suffixes
            return pth
        
        logging.error(f"Unable to parse file property: '{fProperty}")
        return arg

    def _loadMetadata(self) -> dict:
        if not self._metadataPath.exists():
            return {}
        
        with open(self._metadataPath) as fp:
            try:
                return json.load(fp)
            except json.JSONDecodeError:
                return {}

    def _syncMetadata(self) -> None:
        with open(self._metadataPath, "w") as fp:
            json.dump(self._metadata, fp, indent=4)

    def updateMetadata(self, step: Step, stepIndex: int, metadata: dict[Metadata, any]) -> None:
        parsedMetadata = {}
        for key, value in metadata.items():
            if not isinstance(key, Metadata):
                continue

            if key == Metadata.CUSTOM:
                for customKey, customValue in value.items():
                    parsedMetadata[customKey] = customValue

                continue

            parsedMetadata[key.value] = value

        taskMetadata: list[dict] = self._metadata.get(step.value, [])
        if stepIndex < len(taskMetadata):
            taskMetadata[stepIndex] |= parsedMetadata
        else:
            taskMetadata.append(parsedMetadata)

        self._metadata[step.value] = taskMetadata
        self._syncMetadata()

        logging.info(f"Updated {step.value} metadata and saved to file")

    def updateTotalTime(self, totalTime: float, allSucceeded) -> None:
        self._metadata[Metadata.TOTAL_DURATION.value] = totalTime
        if allSucceeded:
            self._metadata[Metadata.LAST_SUCCESS_TOTAL_DURATION.value] = totalTime

        self._syncMetadata()

    def getLastUpdate(self, step: Step) -> datetime | None:
        timestamp = self._metadata[step.value][0][Metadata.LAST_SUCCESS_START.value]
        if timestamp is not None:
            return datetime.fromisoformat(timestamp)

    def getLastOutput(self, step: Step) -> str | None:
        return self._metadata[step.value][-1][Metadata.OUTPUT.value]

    def _getUpdater(self, config: dict) -> upd.Update:
        updaterTypeValue = config.get("type", Updates.WEEKLY.value)
        updaterType = Updates._value2member_map_.get(updaterTypeValue, None)
        if updaterType is None:
            raise Exception(f"Unknown update type: {updaterTypeValue}") from AttributeError
    
        if updaterType == Updates.DAILY:
            return upd.DailyUpdate(config)

        if updaterType == Updates.WEEKLY:
            return upd.WeeklyUpdate(config)
        
        if updaterType == Updates.MONTHLY:
            return upd.MonthlyUpdate(config)
        
        raise Exception(f"Unknown updater type: {updaterType}") from AttributeError
