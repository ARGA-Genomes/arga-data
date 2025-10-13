from lib.config import globalConfig as gcfg
from lib.secrets import secrets
from enum import Enum
from pathlib import Path
from lib.systemManagers.downloading import DownloadManager
from lib.systemManagers.processing import ProcessingManager
from lib.processing.updating import UpdateManager
from lib.crawler import Crawler
import logging
import json

class Step(Enum):
    DOWNLOADING = "downloading"
    PROCESSING  = "processing"

class Flag(Enum):
    VERBOSE   = "quiet" # Verbosity enabled by default, flag is used when silenced
    REPREPARE = "reprepare"
    OVERWRITE = "overwrite"

sourceConfigName = "config.json"

class Database:
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
        self.libDir = self.locationDir / "llib" # Location based lib for shared scripts
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
            self.dataDir = self.config.overwrites.storage / self.dataDir.relative_to(self.locationDir.parent) # Change parent of locationDir (dataSources folder) to storage dir

        # Username/Password
        sourceSecrets = secrets[self.locationDir.name]
        username = sourceSecrets.username if sourceSecrets is not None else ""
        password = sourceSecrets.password if sourceSecrets is not None else ""

        # Location Library
        scriptImportLibs = {".llib": self.libDir}

        # System Managers
        self.downloadManager = DownloadManager(self.dataDir, self.scriptsDir, self.databaseDir, scriptImportLibs, username, password)
        self.processingManager = ProcessingManager(self.dataDir, self.scriptsDir, self.databaseDir, scriptImportLibs)

        # Config stages
        self.downloadConfig: dict = self.config.pop(self.downloadManager.stepName, None)
        self.processingConfig: dict = self.config.pop(self.processingManager.stepName, {})

        if self.downloadConfig is None:
            raise Exception(f"No download config specified as required for {self.name}") from AttributeError
        
        # Updating
        self.updateConfig: dict = self.config.pop("updating", {})
        self.updateManager = UpdateManager(self.updateConfig)

        # Report extra config options
        self._reportLeftovers()

        # Preparation Stage
        self._prepStage = -1

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)
    
    def _reportLeftovers(self) -> None:
        for property in self.config:
            logging.debug(f"{self.name} unknown config item: {property}")

    def _prepareDownload(self, flags: list[Flag]) -> None:
        for file in self.downloadConfig:
            url = file.get("url", None)
            name = file.get("name", None)
            properties = file.get("properties", {})

            if url is None:
                raise Exception("No url provided for source") from AttributeError

            if name is None:
                raise Exception("No filename provided to download to") from AttributeError
            
            self.downloadManager.registerFromURL(url, name, properties)
    
    def _prepareProcessing(self, flags: list[Flag]) -> None:
        parallelProcessing: list[dict] = self.processingConfig.pop("parallel", [])
        linearProcessing: list[dict] = self.processingConfig.pop("linear", [])

        for file in self.downloadManager.getFiles():
            self.processingManager.registerFile(file, parallelProcessing)

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

# class CrawlDB(BasicDB):

#     retrieveType = Retrieve.CRAWL

#     def _prepareDownload(self, flags: list[Flag]) -> None:
#         url = self.downloadConfig.pop("url", None)
#         regex = self.downloadConfig.pop("regex", None)
#         link = self.downloadConfig.pop("link", "")
#         maxDepth = self.downloadConfig.pop("maxDepth", -1)
#         properties = self.downloadConfig.pop("properties", {})
#         filenameURLParts = self.downloadConfig.pop("urlPrefix", 1)

#         crawler = Crawler(self.subsectionDir, self.downloadManager.auth)
#         crawler.run(url, regex, maxDepth, Flag.REPREPARE in flags)
#         urlList = crawler.getFileURLs(link)

#         for url in urlList:
#             fileName = "_".join(url.split("/")[-filenameURLParts:])
#             self.downloadManager.registerFromURL(url, fileName, properties)

# class ScriptDB(BasicDB):

#     retrieveType = Retrieve.SCRIPT

#     def _prepareDownload(self, flags: list[Flag]) -> None:
#         self.downloadManager.registerFromScript(self.downloadConfig)
