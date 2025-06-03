from lib.config import globalConfig as gcfg
from lib.secrets import secrets
from enum import Enum
from pathlib import Path

from lib.systemManagers.downloading import DownloadManager
from lib.systemManagers.processing import ProcessingManager
from lib.systemManagers.conversion import ConversionManager
from lib.systemManagers.updating import UpdateManager

from lib.processing.stages import Step

from lib.crawler import Crawler
import logging

class Retrieve(Enum):
    URL     = "url"
    CRAWL   = "crawl"
    SCRIPT  = "script"

class Flag(Enum):
    VERBOSE = "verbose"
    PREPARE_OVERWRITE = "reprepare"
    OUTPUT_OVERWRITE = "overwrite"
    UPDATE = "update"

class BasicDB:

    retrieveType = Retrieve.URL

    def __init__(self, location: str, database: str, subsection: str, datasetID: str, config: dict):
        self.location = location
        self.database = database
        self.subsection = subsection
        self.datasetID = datasetID

        # Relative folders
        self.locationDir = gcfg.folders.dataSources / location
        self.databaseDir = self.locationDir / database
        self.subsectionDir = self.databaseDir / self.subsection # If no subsection, does nothing

        self.config = gcfg
        for dir in (self.locationDir, self.databaseDir, self.subsectionDir):
            subdirConfig = Path(dir / "config.toml")
            if subdirConfig.exists():
                self.config = self.config.createChild(subdirConfig)

        self.dataDir = self.subsectionDir / "data" if not self.config.overwrites.storage else self.config.overwrites.storage / location / database / self.subsection / "data"

        # Username/Password
        sourceSecrets = secrets[self.location]
        username = sourceSecrets.username if sourceSecrets is not None else ""
        password = sourceSecrets.password if sourceSecrets is not None else ""

        # System Managers
        self.downloadManager = DownloadManager(self.subsectionDir, self.dataDir, username, password)
        self.processingManager = ProcessingManager(self.subsectionDir, self.dataDir, self.locationDir)
        self.conversionManager = ConversionManager(self.subsectionDir, self.dataDir, self.datasetID, location, database, subsection)

        # Config stages
        self.downloadConfig: dict = config.pop(self.downloadManager.stepName, None)
        self.processingConfig: dict = config.pop(self.processingManager.stepName, {})
        self.conversionConfig: dict = config.pop(self.conversionManager.stepName, {})

        if self.downloadConfig is None:
            raise Exception("No download config specified as required") from AttributeError
        
        # Updating
        self.updateConfig: dict = config.pop("updating", {})
        self.updateManager = UpdateManager(self.updateConfig)

        # Report extra config options
        self._reportLeftovers(config)

        # Preparation Stage
        self._prepStage = -1

    def __str__(self):
        return f"{self.location}-{self.database}{'-' + self.subsection if self.subsection else ''}"

    def __repr__(self):
        return str(self)
    
    def _reportLeftovers(self, properties: dict) -> None:
        for property in properties:
            logging.debug(f"{self.location}-{self.database} unknown config item: {property}")

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

        self.processingManager.addFinalProcessing(linearProcessing)
    
    def _prepareConversion(self, flags: list[Flag]) -> None:
        fileToConvert = self.processingManager.getLatestNodeFile()
        self.conversionManager.loadFile(fileToConvert, self.conversionConfig, self.databaseDir)

    def _prepare(self, step: Step, flags: list[Flag]) -> bool:
        callbacks = {
            Step.DOWNLOAD: self._prepareDownload,
            Step.PROCESSING: self._prepareProcessing,
            Step.CONVERSION: self._prepareConversion
        }

        if step not in callbacks:
            raise Exception(f"Uknown step to prepare: {step}")

        for idx, (stepType, callback) in enumerate(callbacks.items()):
            if idx <= self._prepStage:
                continue

            logging.info(f"Preparing {self} step '{stepType.name}' with flags: {self._verboseFlags(flags)}")
            try:
                callback(flags)
            except AttributeError as e:
                logging.error(f"Error preparing step: {stepType.name} - {e}")
                return False
            
            self._prepStage = idx
            if step == stepType:
                break
            
        return True

    def _execute(self, step: Step, flags: list[Flag], **kwargs: dict) -> bool:
        overwrite = Flag.OUTPUT_OVERWRITE in flags
        verbose = Flag.VERBOSE in flags

        logging.info(f"Executing {self} step '{step.name}' with flags: {self._verboseFlags(flags)}")
        if step == Step.DOWNLOAD:
            return self.downloadManager.download(overwrite, verbose, **kwargs)

        if step == Step.PROCESSING:
            return self.processingManager.process(overwrite, verbose, **kwargs)
        
        if step == Step.CONVERSION:
            return self.conversionManager.convert(overwrite, verbose, **kwargs)

        logging.error(f"Unknown step to execute: {step}")
        return False
    
    def create(self, step: Step, flags: list[Flag], **kwargs: dict) -> None:
        try:
            success = self._prepare(step, flags)
            if not success:
                return
            
        except KeyboardInterrupt:
            logging.info(f"Process ended early when attempting to prepare step '{step.name}' for {self}")

        try:
            self._execute(step, flags, **kwargs)
        except KeyboardInterrupt:
            logging.info(f"Process ended early when attempting to execute step '{step.name}' for {self}")

    def package(self) -> Path:
        outputDir = self.localConfigfolders.package if isinstance(self.localConfigfolders.package, Path) else self.dataDir
        outputPath = self.conversionManager.package(outputDir)
        if outputPath is not None:
            logging.info(f"Successfully zipped converted data source file to {outputPath}")

        return outputPath

    def checkUpdateReady(self) -> bool:
        lastUpdate = self.downloadManager.getLastUpdate()
        return self.updateManager.isUpdateReady(lastUpdate)
    
    def update(self, flags: list[Flag]) -> bool:
        for flag in [Flag.UPDATE, Flag.OUTPUT_OVERWRITE]:
            if flag not in flags:
                flags.append(flag)

        for step in (Step.DOWNLOAD, Step.PROCESSING, Step.CONVERSION):
            self.create(step, flags)

        self.package()

    def _verboseFlags(self, flags: list[Flag]) -> str:
        return " | ".join(f"{flag.value}={flag in flags}" for flag in Flag)

class CrawlDB(BasicDB):

    retrieveType = Retrieve.CRAWL

    def _prepareDownload(self, flags: list[Flag]) -> None:
        url = self.downloadConfig.pop("url", None)
        regex = self.downloadConfig.pop("regex", None)
        link = self.downloadConfig.pop("link", "")
        maxDepth = self.downloadConfig.pop("maxDepth", -1)
        properties = self.downloadConfig.pop("properties", {})
        filenameURLParts = self.downloadConfig.pop("urlPrefix", 1)

        crawler = Crawler(self.subsectionDir)
        crawler.run(url, regex, maxDepth, Flag.PREPARE_OVERWRITE in flags)
        urlList = crawler.getFileURLs(link)

        for url in urlList:
            fileName = "_".join(url.split("/")[-filenameURLParts:])
            self.downloadManager.registerFromURL(url, fileName, properties)

class ScriptDB(BasicDB):

    retrieveType = Retrieve.SCRIPT

    def _prepareDownload(self, flags: list[Flag]) -> None:
        self.downloadManager.registerFromScript(self.downloadConfig)
