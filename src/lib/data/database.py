import json
import logging
import lib.settings as settings
import lib.secrets as scr
from enum import Enum
from pathlib import Path
from lib.processing import tasks
import lib.processing.updating as upd
import lib.processing.parsing as parse
import time
from datetime import datetime
from lib.processing.files import DataFile
from lib.json import JsonSynchronizer
import traceback

sourceConfigName = "config.json"

class Flag(Enum):
    VERBOSE   = "quiet" # Verbosity enabled by default, flag is used when silenced
    REPREPARE = "reprepare"
    OVERWRITE = "overwrite"

class Step(Enum):
    DOWNLOADING = "downloading"
    PROCESSING  = "processing"
    COMPILING = "compiling"

class Retrieve(Enum):
    URL     = "url"
    CRAWL   = "crawl"
    SCRIPT  = "script"

class Updates(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

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

        self.subsections: dict[str, str] | list[str] = config.pop("subsections", {})
        self.config = config

    def locationName(self) -> str:
        return self.locationDir.name

    def databaseName(self) -> str:
        return self.databaseDir.name

    def listSubsections(self) -> list[str]:
        return list(self.subsections)

    def constuct(self, name: str, subsection: str):
        self.name = name
        self.subsection = subsection # Subsection is verified before construction, so is valid
        self.subsectionDir = self.databaseDir / subsection # Same as databaseDir if no subsection
        self.locationDir = self.databaseDir.parent

        # Subsection remapping
        if subsection:
            rawConfig = json.dumps(self.config)
            rawConfig = rawConfig.replace("<SUB>", subsection)

            if isinstance(self.subsections, dict): # Name provided with subsections
                 rawConfig = rawConfig.replace("<SUB:VALUE>", self.subsections[subsection])

            self.config = json.loads(rawConfig)

        # Local storage and libraries
        self.exampleDir = self.subsectionDir / "examples" # Data sample storage location
        self.dirLookup = parse.DirLookup({
            ".": self.databaseDir / "scripts",
            ".lib": settings.rootDir / "src" / "lib",
            ".llib": self.locationDir / "llib"
        })

        # Local settings
        self.settings = settings.load()
        for dir in (self.locationDir, self.databaseDir, self.subsectionDir):
            subdirConfig = Path(dir / "settings.toml")
            if subdirConfig.exists():
                self.settings = self.settings.createChild(subdirConfig)

        # Data storage
        self.dataDir = self.subsectionDir / "data" # Default data location
        if self.settings.storage.data: # Overwrite data location
            self.dataDir: Path = self.settings.storage.data / self.dataDir.relative_to(self.locationDir.parent) # Change parent of locationDir (dataSources folder) to storage dir

        self.workingDirs = {step: self.dataDir / step.value for step in Step}

        # Tasks
        self._queuedTasks: dict[Step, list[tasks.Task]] = {step: [] for step in Step}
        self._metadata = JsonSynchronizer(self.subsectionDir / self._metadataFileName)

        # Updating
        updateConfig: dict = self.config.pop("updating", {})
        self._update = self._getUpdater(updateConfig)

        # Preparation Stage
        self._lastPrepared = None

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)
    
    def _reportLeftovers(self, config: dict, sectionName: str) -> None:
        for property in config:
            logging.debug(f"{self.name} unknown{f' {sectionName}' if sectionName else ''} config item: {property}")

    def _flattenTaskOutputs(self, taskList: list[tasks.Task]) -> list[DataFile]:
        return [output for task in taskList for output in task.getOutputs()]
    
    def _getCurrentLookup(self) -> parse.DataFileLookup:
        inputs = self._flattenTaskOutputs(self._queuedTasks[Step.PROCESSING][-1:] or self._queuedTasks[Step.DOWNLOADING][-1:])
        downloads = self._flattenTaskOutputs(self._queuedTasks[Step.DOWNLOADING])
        processed = self._flattenTaskOutputs(self._queuedTasks[Step.PROCESSING])

        return parse.DataFileLookup(inputs, downloads, processed)

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
        secrets = scr.load()
        sourceSecrets = secrets[self.locationDir.name]
        username = sourceSecrets.username if sourceSecrets is not None else ""
        password = sourceSecrets.password if sourceSecrets is not None else ""

        overwrite = Flag.REPREPARE in flags

        retrieve = Retrieve._value2member_map_.get(retrieveType, None)
        for taskConfig in downloadTaskConfig:
            if retrieve == Retrieve.URL:
                self._queuedTasks[Step.DOWNLOADING].append(tasks.UrlRetrieve(self.workingDirs[Step.DOWNLOADING], taskConfig, username, password))

            elif retrieve == Retrieve.CRAWL:
                self._queuedTasks[Step.DOWNLOADING].append(tasks.CrawlRetrieve(self.workingDirs[Step.DOWNLOADING], taskConfig, username, password, overwrite))

            elif retrieve == Retrieve.SCRIPT:
                self._queuedTasks[Step.DOWNLOADING].append(tasks.ScriptRunner(self.workingDirs[Step.DOWNLOADING], taskConfig, self.dirLookup, self._getCurrentLookup()))

            else:
                raise Exception(f"Unknown retrieve type '{retrieveType}' specified for {self.name}") from AttributeError

    def _prepareProcessing(self, flags: list[Flag]) -> None:
        processingConfig: list[dict] = self.config.pop(Step.PROCESSING.value, [])

        for processingStep in processingConfig:
            self._queuedTasks[Step.PROCESSING].append(tasks.ScriptRunner(self.workingDirs[Step.PROCESSING], processingStep, self.dirLookup, self._getCurrentLookup()))

    def _prepareCompiling(self, flags: list[Flag]) -> None:
        compilingConfig: dict = self.config.pop(Step.COMPILING, {})

        inputs = self._queuedTasks[Step.PROCESSING][-1:] or self._queuedTasks[Step.DOWNLOADING][-1:]
        downloads = self._flattenTaskOutputs(self._queuedTasks[Step.DOWNLOADING])
        processed = self._flattenTaskOutputs(self._queuedTasks[Step.PROCESSING])
        fileLookup = parse.DataFile(inputs, downloads, processed)

        self._queuedTasks[Step.COMPILING].append(tasks.Compiler(self.workingDirs[Step.COMPILING], compilingConfig, self.name, self.dirLookup, fileLookup))

    def _prepare(self, fileStep: Step, flags: list[Flag]) -> bool:
        stepMap = {
            Step.DOWNLOADING: (None, self._prepareDownload),
            Step.PROCESSING: (Step.DOWNLOADING, self._prepareProcessing),
            Step.COMPILING: (Step.PROCESSING, self._prepareCompiling)
        }

        if fileStep not in stepMap:
            raise Exception(f"Unknown step to prepare: {fileStep}")

        for stepType, (requirement, callback) in stepMap.items():
            if self._lastPrepared != requirement:
                continue

            logging.info(f"Preparing {self} step '{stepType.name}' with flags: {self._printFlags(flags)}")
            try:
                callback(flags)
            except Exception as e:
                logging.error(f"Error preparing step \'{stepType.name}\'. Reason: {e}")
                logging.error(traceback.format_exc())
                return False
            
            self._lastPrepared = stepType
            if fileStep is stepType:
                break
            
        return True
    
    def _execute(self, step: Step, flags: list[Flag]) -> bool:
        overwrite = Flag.OVERWRITE in flags
        verbose = Flag.VERBOSE in flags
        logging.info(f"Executing {self} step '{step.name}' with flags: {self._printFlags(flags)}")

        evaluationTasks = self._queuedTasks.get(step, [])
        if not evaluationTasks:
            logging.info(f"No tasks to evaluate for step {step.value}")
            return True

        workingDir = self.workingDirs[step]
        workingDir.mkdir(parents=True, exist_ok=True)

        allSucceeded = False
        startTime = time.perf_counter()
        for idx, task in enumerate(evaluationTasks):
            taskStartTime = time.perf_counter()
            taskStartDate = datetime.now().isoformat()

            try:
                success = task.run(overwrite, verbose)
            except KeyboardInterrupt:
                logging.info("Cancelling task execution early")
                return

            duration = time.perf_counter() - taskStartTime
            taskEndDate = datetime.now().isoformat()

            packet = {
                Metadata.OUTPUTS: [t.path.name for t in task.getOutputs()],
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
        if not isinstance(taskMetadata, list):
            taskMetadata = []

        if stepIndex < len(taskMetadata):
            taskMetadata[stepIndex] |= parsedMetadata
        else:
            taskMetadata.append(parsedMetadata)

        self._metadata[step.value] = taskMetadata

        logging.info(f"Updated {step.value} metadata and saved to file")

    def updateTotalTime(self, totalTime: float, allSucceeded) -> None:
        self._metadata[Metadata.TOTAL_DURATION.value] = totalTime
        if allSucceeded:
            self._metadata[Metadata.LAST_SUCCESS_TOTAL_DURATION.value] = totalTime

    def getLastUpdate(self, step: Step) -> datetime | None:
        timestamp = self._metadata[step.value][0][Metadata.LAST_SUCCESS_START.value]
        if timestamp is not None:
            return datetime.fromisoformat(timestamp)

    def getLastOutputs(self, step: Step) -> list[str]:
        return self._metadata.get(step.value, [])[-1].get(Metadata.OUTPUTS.value, [])

    def _getUpdater(self, config: dict) -> upd.Update:
        updaterTypeValue = config.get("type", None)
        if updaterTypeValue is None:
            raise Exception("No update config specified") from AttributeError

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
