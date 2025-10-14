from lib.config import globalConfig as gcfg
from lib.secrets import secrets
from enum import Enum
from pathlib import Path
from lib.processing.updating import UpdateManager
from lib.processing.metadata import MetadataManager
import lib.processing.tasks as tasks
import logging
import json

class Step(Enum):
    DOWNLOADING = "downloading"
    PROCESSING  = "processing"

class Retrieve(Enum):
    URL     = "url"
    CRAWL   = "crawl"
    SCRIPT  = "script"

class Flag(Enum):
    VERBOSE   = "quiet" # Verbosity enabled by default, flag is used when silenced
    REPREPARE = "reprepare"
    OVERWRITE = "overwrite"

sourceConfigName = "config.json"

class Database:

    _retrieveType = "retrieveType"
    _downloadTasks = "tasks"

    _parallelProcessing = "parallel"
    _linearProcessing = "linear"

    _localLibrary = "llib"

    def __init__(self, databaseDir: Path):
        self.databaseDir = databaseDir

        configPath = databaseDir / sourceConfigName
        if configPath.exists():
            with open(configPath) as fp:
                configData = json.load(fp)
        else:
            configData = {}

        self.subsections: dict[str, dict[str, str]] = configData.pop("subsections", {})
        self.configData = configData

    def shortName(self) -> str:
        return self.databaseDir.name

    def listSubsections(self) -> list[str]:
        return list(self.subsections)

    def constuct(self, name: str, subsection: str):
        self.name = name
        self.locationDir = self.databaseDir.parent
        self.databaseDir = self.databaseDir
        self.subsectionDir = self.databaseDir / subsection # Same as databaseDir if no subsection

        # Subsection remapping
        if subsection:
            rawConfig = json.dumps(self.configData)
            rawConfig = rawConfig.replace("<SUB>", subsection)
            tags = self.subsections.get("tags", {})
            for tag, replaceValue in tags.items():
                rawConfig = rawConfig.replace(f"<SUB:{tag.upper()}>", replaceValue)

            self.config = json.loads(rawConfig)

        # Local storage
        self.libDir = self.locationDir / self._localLibrary # Location based lib for shared scripts
        self.scriptsDir = self.databaseDir / "scripts" # Database specific scripts
        self.exampleDir = self.subsectionDir / "examples" # Data sample storage location

        # Local configs
        self.config = gcfg
        for dir in (self.locationDir, self.databaseDir, self.subsectionDir):
            subdirConfig = Path(dir / "config.toml")
            if subdirConfig.exists():
                self.config = self.config.createChild(subdirConfig)

        # Data storage
        self.dataDir = self.subsectionDir / "data" # Default data location
        if self.config.overwrites.storage: # Overwrite data location
            self.dataDir: Path = self.config.overwrites.storage / self.dataDir.relative_to(self.locationDir.parent) # Change parent of locationDir (dataSources folder) to storage dir

        self.downloadDir = self.dataDir / Step.DOWNLOADING.value
        self.processingDir = self.dataDir / Step.PROCESSING.value

        # Tasks
        self._queuedTasks: dict[Step, list[tasks.Task]] = {}

        # Metadata
        self.metadataManager = MetadataManager(self.databaseDir)

        # Updating
        self.updateConfig: dict = self.config.pop("updating", {})
        self.updateManager = UpdateManager(self.updateConfig)

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
        downloadConfig: dict = self.configData.pop(Step.DOWNLOADING.value, {})
        if not downloadConfig:
            raise Exception(f"No download config specified as required for {self.name}") from AttributeError

        retrieveType = downloadConfig.pop(self._retrieveType, None)
        if retrieveType is None:
            raise Exception(f"No retrieve type specified in download config for {self.name}") from AttributeError
        
        downloadTasks = downloadConfig.pop(self._downloadTasks, None)
        if downloadTasks is None:
            raise Exception(f"No download tasks specified in download config for {self.name}") from AttributeError

        # Get username/password for url/crawl downloads
        sourceSecrets = secrets[self.locationDir.name]
        username = sourceSecrets.username if sourceSecrets is not None else ""
        password = sourceSecrets.password if sourceSecrets is not None else ""

        overwrite = Flag.REPREPARE in flags

        retrieve = Retrieve._value2member_map_.get(retrieveType, None)
        if retrieve == Retrieve.URL: # Tasks should be a list of dicts
            if not isinstance(downloadTasks, list):
                raise Exception(f"URL retrieve type expects a list of task configs for {self.name}") from AttributeError

            for taskConfig in downloadTasks:
                self._queuedTasks[Step.DOWNLOADING].append(tasks.URLDownload(self.downloadDir, username, password, taskConfig))

        if retrieveType == Retrieve.CRAWL: # Tasks should be dict
            if not isinstance(downloadTasks, dict):
                raise Exception(f"Crawl retrieve type expects a dict config for {self.name}")
            
            self._queuedTasks[Step.DOWNLOADING].append(tasks.CrawlDownload(self.downloadDir, username, password, downloadTasks, overwrite))

        if retrieveType == Retrieve.SCRIPT: # Tasks should be dict
            if not isinstance(downloadTasks, dict):
                raise Exception(f"Script retrieve type expects a dict config for {self.name}")
            
            self._queuedTasks[Step.DOWNLOADING].append(tasks.ScriptDownload(self.scriptsDir, self.downloadDir, self.libDir, downloadTasks))

        raise Exception(f"Unknown retrieve type '{retrieveType}' specified for {self.name}") from AttributeError

    def _prepareProcessing(self, flags: list[Flag]) -> None:
        processingConfig: dict = self.configData.pop(Step.PROCESSING.value, {})

        parallelProcessing: list[dict] = processingConfig.pop(self._parallelProcessing, [])
        for task in self._queuedTasks[Step.DOWNLOADING]:
            for output in task.getOutputs():
                self._queuedTasks[Step.PROCESSING]

            self.processingManager.registerFile(file, parallelProcessing)

        linearProcessing: list[dict] = processingConfig.pop(self._linearProcessing, [])
        if linearProcessing:
            self.processingManager.addFinalProcessing(linearProcessing)

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

    def _execute(self, fileStep: Step, flags: list[Flag]) -> bool:
        overwrite = Flag.OVERWRITE in flags
        verbose = Flag.VERBOSE in flags

        logging.info(f"Executing {self} step '{fileStep.name}' with flags: {self._printFlags(flags)}")

        if fileStep == Step.DOWNLOADING:
            return self.downloadManager.download(overwrite, verbose)

        if fileStep == Step.PROCESSING:
            return self.processingManager.process(overwrite, verbose)

        logging.error(f"Unknown step to execute: {fileStep}")
        return False
    
    def create(self, fileStep: Step, flags: list[Flag]) -> None:
        try:
            success = self._prepare(fileStep, flags)
            if not success:
                return
            
        except KeyboardInterrupt:
            logging.info(f"Process ended early when attempting to prepare step '{fileStep.name}' for {self}")

        try:
            self._execute(fileStep, flags)
        except KeyboardInterrupt:
            logging.info(f"Process ended early when attempting to execute step '{fileStep.name}' for {self}")

    def checkUpdateReady(self) -> bool:
        lastUpdate = self.downloadManager.getLastUpdate()
        return self.updateManager.isUpdateReady(lastUpdate)
    
    def update(self, flags: list[Flag]) -> bool:
        for fileStep in (Step.DOWNLOADING, Step.PROCESSING):
            self.create(fileStep, list(set(flags).add(Flag.OVERWRITE)))

        self.package()

    def _printFlags(self, flags: list[Flag]) -> str:
        return " | ".join(f"{flag.value}={flag in flags}" for flag in Flag)
