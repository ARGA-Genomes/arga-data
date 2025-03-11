import lib.config as cfg
from enum import Enum
from pathlib import Path

from lib.systemManagers.downloading import DownloadManager
from lib.systemManagers.processing import ProcessingManager
from lib.systemManagers.conversion import ConversionManager
from lib.systemManagers.updating import UpdateManager

from lib.processing.stages import Step

from lib.tools.crawler import Crawler
from lib.tools.logger import Logger

class Retrieve(Enum):
    URL     = "url"
    CRAWL   = "crawl"
    SCRIPT  = "script"

class BasicDB:

    retrieveType = Retrieve.URL

    def __init__(self, location: str, database: str, subsection: str, datasetID: str, config: dict):
        self.location = location
        self.database = database
        self.subsection = subsection
        self.datasetID = datasetID

        # Auth
        self.authFile: str = config.pop("auth", "")

        # Relative folders
        self.locationDir = cfg.Folders.dataSources / location
        self.databaseDir = self.locationDir / database
        self.subsectionDir = self.databaseDir / self.subsection # If no subsection, does nothing
        self.dataDir = self.subsectionDir / "data"

        # System Managers
        self.downloadManager = DownloadManager(self.dataDir, self.authFile)
        self.processingManager = ProcessingManager(self.dataDir)
        self.conversionManager = ConversionManager(self.dataDir, self.datasetID, location, database, subsection)

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
            Logger.debug(f"{self.location}-{self.database} unknown config item: {property}")

    def _prepareDownload(self, overwrite: bool, verbose: bool) -> None:
        files: list[dict] = self.downloadConfig.pop("files", [])

        for file in files:
            url = file.get("url", None)
            name = file.get("name", None)
            properties = file.get("properties", {})

            if url is None:
                raise Exception("No url provided for source") from AttributeError

            if name is None:
                raise Exception("No filename provided to download to") from AttributeError
            
            self.downloadManager.registerFromURL(url, name, properties)
    
    def _prepareProcessing(self, overwrite: bool, verbose: bool) -> None:
        firstProcessing: dict[int, list[dict]] = self.processingConfig.pop("first", [])
        specificProcessing: list[dict] = self.processingConfig.pop("specific", {})
        finalProcessing: list[dict] = self.processingConfig.pop("final", [])

        for file in self.downloadManager.getFiles():
            self.processingManager.registerFile(file, firstProcessing)

        self.processingManager.addSpecificProcessing(specificProcessing)
        self.processingManager.addFinalProcessing(finalProcessing)
    
    def _prepareConversion(self, overwrite: bool, verbose: bool) -> None:
        fileToConvert = self.processingManager.getLatestNodeFile()
        self.conversionManager.loadFile(fileToConvert, self.conversionConfig, self.databaseDir)

    def _prepare(self, step: Step, overwrite: bool, verbose: bool) -> bool:
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

            Logger.info(f"Preparing {self} step '{stepType.name}' with flags: overwrite={overwrite} | verbose={verbose}")
            try:
                callback(overwrite if step == stepType else False, verbose)
            except AttributeError as e:
                Logger.error(f"Error preparing step: {stepType.name} - {e}")
                return False
            
            self._prepStage = idx
            if step == stepType:
                break
            
        return True

    def _execute(self, step: Step, overwrite: bool, verbose: bool, **kwargs: dict) -> bool:
        Logger.info(f"Executing {self} step '{step.name}' with flags: overwrite={overwrite} | verbose={verbose}")
        if step == Step.DOWNLOAD:
            return self.downloadManager.download(overwrite, verbose, **kwargs)

        if step == Step.PROCESSING:
            return self.processingManager.process(overwrite, verbose, **kwargs)
        
        if step == Step.CONVERSION:
            return self.conversionManager.convert(overwrite, verbose, **kwargs)

        Logger.error(f"Unknown step to execute: {step}")
        return False
    
    def create(self, step: Step, overwrite: tuple[bool, bool], verbose: bool, **kwargs: dict) -> None:
        prepare, reprocess = overwrite

        try:
            success = self._prepare(step, prepare, verbose)
            if not success:
                return
        except KeyboardInterrupt:
            Logger.info(f"Process ended early when attempting to prepare step '{step.name}' for {self}")

        try:
            self._execute(step, reprocess, verbose, **kwargs)
        except KeyboardInterrupt:
            Logger.info(f"Process ended early when attempting to execute step '{step.name}' for {self}")

    def package(self) -> Path:
        outputPath = self.conversionManager.package(self.dataDir)
        if outputPath is not None:
            Logger.info(f"Successfully zipped converted data source file to {outputPath}")

        return outputPath

    def checkUpdateReady(self) -> bool:
        lastUpdate = self.downloadManager.getLastUpdate()
        return self.updateManager.isUpdateReady(lastUpdate)
    
    def update(self) -> bool:
        for step in (Step.DOWNLOAD, Step.PROCESSING, Step.CONVERSION):
            self.create(step, (True, True), True)

class CrawlDB(BasicDB):

    retrieveType = Retrieve.CRAWL

    def _prepareDownload(self, overwrite: bool, verbose: bool) -> None:
        properties = self.downloadConfig.pop("properties", {})
        folderPrefix = self.downloadConfig.pop("prefix", False)
        saveFile = self.downloadConfig.pop("saveFile", "crawl.txt")
        saveFilePath: Path = self.subsectionDir / saveFile

        if saveFilePath.exists() and not overwrite:
            Logger.info("Local file found, skipping crawling")
            with open(saveFilePath) as fp:
                urls = fp.read().splitlines()
        else:
            saveFilePath.unlink(True)

            urls = self._crawl(saveFilePath.parent)
            saveFilePath.parent.mkdir(parents=True, exist_ok=True) # Create base directory if it doesn't exist to put the file
            with open(saveFilePath, 'w') as fp:
                fp.write("\n".join(urls))

        for url in urls:
            fileName = self._getFileNameFromURL(url, folderPrefix)
            self.downloadManager.registerFromURL(url, fileName, properties)

    def _crawl(self, crawlerDirectory: Path) -> None:
        url = self.downloadConfig.pop("url", None)
        regex = self.downloadConfig.pop("regex", ".*")
        link = self.downloadConfig.pop("link", "")
        maxDepth = self.downloadConfig.pop("maxDepth", -1)

        crawler = Crawler(crawlerDirectory, regex, link, maxDepth, user=self.downloadManager.username, password=self.downloadManager.password)

        if url is None:
            raise Exception("No file location for source") from AttributeError
        
        crawler.crawl(url, True)
        return crawler.getURLList()
    
    def _getFileNameFromURL(self, url: str, folderPrefix: bool) -> str:
        urlParts = url.split('/')
        fileName = urlParts[-1]

        if not folderPrefix:
            return fileName

        folderName = urlParts[-2]
        return f"{folderName}_{fileName}"

class ScriptDB(BasicDB):

    retrieveType = Retrieve.SCRIPT

    def _prepareDownload(self, overwrite: bool, verbose: bool) -> None:
        self.downloadManager.registerFromScript(self.downloadConfig)
