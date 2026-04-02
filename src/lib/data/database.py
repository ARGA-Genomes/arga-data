import json
import logging
from lib.settings import Settings
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
    CONVERSION  = "conversion"

class Retrieve(Enum):
    URL     = "url"
    CRAWL   = "crawl"
    SCRIPT  = "script"

class Updates(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"

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
        settings = Settings()

        self.exampleDir = self.subsectionDir / "examples" # Data sample storage location
        self.dirLookup = {
            ".": settings.scriptsDir / self.locationName(),
            ".lib": settings.libDir,
        }

        # Local settings
        for dir in (self.locationDir, self.databaseDir, self.subsectionDir):
            subdirConfig = Path(dir / "settings.toml")
            if subdirConfig.exists():
                settings.update(subdirConfig)

        # Data storage
        self.dataDir = self.subsectionDir / "data" # Default data location
        if settings.Storage.DATA: # Overwrite data location
            self.dataDir: Path = settings.Storage.DATA / self.dataDir.relative_to(self.locationDir.parent) # Change parent of locationDir (dataSources folder) to storage dir

        self.workingDirs: dict[Step, Path] = {}
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

    def _generateWorkingDirs(self, historicFolderNum) -> bool:

        def _generate(folder: Path) -> None:
            self.workingDirs = {step: folder / step.value for step in Step}

        todaysDataDir = self.dataDir / str(datetime.now().date())   
        if historicFolderNum == -1: # Force today
            _generate(todaysDataDir)
            return True

        historicFolders = sorted(item for item in self.dataDir.iterdir() if item.is_dir())
        if historicFolderNum > (len(historicFolders) - 1):
            logging.error(f"Unable to select historic folder #{historicFolderNum} as there are only {len(historicFolders)}")
            return False
        
        _generate(historicFolders[historicFolderNum])
        return True

    def _getDownloaded(self) -> list[list[DataFile]]:
        ...

    def _getProcessed(self) -> list[list[DataFile]]:
        ...

    def download(self, flags: list[Flag]) -> None:
        downloadConfig: dict = self.config.pop(Step.DOWNLOADING.value, {})
        if not downloadConfig:
            raise Exception(f"No download config specified as required for {self.name}") from AttributeError

        retrieveType = downloadConfig.pop("retrieveType", None)
        if retrieveType is None:
            raise Exception(f"No retrieve type specified in download config for {self.name}") from AttributeError
        
        downloadTaskConfig = downloadConfig.pop("tasks", None)
        if downloadTaskConfig is None:
            raise Exception(f"No download tasks specified in download config for {self.name}") from AttributeError
        
        if not self._generateWorkingDirs(-1):
            return
        
        downloadTasks = []
        retrieve = Retrieve._value2member_map_.get(retrieveType)
        for taskConfig in downloadTaskConfig:
            if retrieve == Retrieve.URL:
                downloadTasks.append(tasks.UrlRetrieve(self.workingDirs[Step.DOWNLOADING], taskConfig, self.locationDir.name))

            elif retrieve == Retrieve.CRAWL:
                downloadTasks.append(tasks.CrawlRetrieve(self.workingDirs[Step.DOWNLOADING], taskConfig, self.locationDir.name, Flag.REPREPARE in flags))

            elif retrieve == Retrieve.SCRIPT:
                downloadTasks.append(tasks.ScriptRunner(self.workingDirs[Step.DOWNLOADING], taskConfig, self.dirLookup, self._getDownloaded(), []))

            else:
                raise Exception(f"Unknown retrieve type '{retrieveType}' specified for {self.name}") from AttributeError
            
        self._execute(Step.DOWNLOADING, downloadTasks, flags)

    def process(self, flags: list[Flag], historicFolderNum: int) -> None:
        if not self._generateWorkingDirs(historicFolderNum):
            return
        
        processingConfig: list[dict] = self.config.pop(Step.PROCESSING.value, [])

        processTasks = []
        for processingStep in processingConfig:
            processTasks.append(tasks.ScriptRunner(self.workingDirs[Step.PROCESSING], processingStep, self.dirLookup, self._getDownloaded(), self._getProcessed()))

        self._execute(Step.PROCESSING, processTasks, flags)

    def convert(self, flags: list[Flag], historicFolderNum) -> None:
        conversionConfig: dict = self.config.pop(Step.CONVERSION.value, {})
        if not conversionConfig:
            raise Exception(f"No conversion config specified as required for {self.name}") from AttributeError
        
        if not self._generateWorkingDirs(historicFolderNum):
            return

        self._execute(Step.PROCESSING, [tasks.Conversion(self.workingDirs[Step.CONVERSION], self.databaseDir, conversionConfig, self.locationName(), self.name, self.subsection, Flag.REPREPARE in flags)], flags)
    
    def _execute(self, step: Step, taskList: list[tasks.Task], flags: list[Flag]) -> bool:
        overwrite = Flag.OVERWRITE in flags
        verbose = Flag.VERBOSE in flags

        logging.info(f"Executing {self} step '{step.name}' with flags: {self._printFlags(flags)}")

        if not taskList:
            logging.info(f"No tasks to evaluate for step {step.value}")
            return True

        workingDir = self.workingDirs[step]
        workingDir.mkdir(parents=True, exist_ok=True)

        allSucceeded = True
        startTime = time.perf_counter()
        for idx, task in enumerate(taskList):
            metadata = task.run(overwrite, verbose)
            self.updateMetadata(step, idx, metadata)

            allSucceeded &= metadata[tasks.Metadata.SUCCESS]

        self.updateTotalTime(time.perf_counter() - startTime, allSucceeded)
        return allSucceeded

    def checkUpdateReady(self) -> bool:
        return self._update.updateReady(self.getLastUpdate(Step.DOWNLOADING))
    
    def update(self, flags: list[Flag]) -> bool:
        for fileStep in (Step.DOWNLOADING, Step.PROCESSING):
            self.create(fileStep, list(set(flags).add(Flag.OVERWRITE)))

    def _printFlags(self, flags: list[Flag]) -> str:
        return " | ".join(f"{flag.value}={flag in flags}" for flag in Flag)

    def updateMetadata(self, step: Step, stepIndex: int, metadata: dict[tasks.Metadata, any]) -> None:
        parsedMetadata = {}
        for key, value in metadata.items():
            if not isinstance(key, tasks.Metadata):
                continue

            if key == tasks.Metadata.CUSTOM:
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
        self._metadata[tasks.Metadata.TOTAL_DURATION.value] = totalTime
        if allSucceeded:
            self._metadata[tasks.Metadata.LAST_SUCCESS_TOTAL_DURATION.value] = totalTime

    def getLastUpdate(self, step: Step) -> datetime | None:
        timestamp = self._metadata[step.value][0][tasks.Metadata.LAST_SUCCESS_START.value]
        if timestamp is not None:
            return datetime.fromisoformat(timestamp)

    def getLastOutputs(self, step: Step) -> list[str]:
        return self._metadata.get(step.value, [])[-1].get(tasks.Metadata.OUTPUTS.value, [])

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
