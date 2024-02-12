import lib.config as cfg
from pathlib import Path
from enum import Enum
from lib.sourceObjs.systemManager import SystemManager
from lib.sourceObjs.timeManager import TimeManager
from lib.processing.stageFile import StageFile, StageFileStep
from lib.tools.crawler import Crawler
from lib.tools.logger import Logger

class DBType(Enum):
    UNKNOWN    = -1
    SPECIFIC   = 0
    LOCATION   = 1
    SCRIPTURL  = 2
    SCRIPTDATA = 3

class Database:
    def __init__(self, location: str, database: str, properties: dict = {}):
        self.location = location
        self.database = database
        self.dbType = DBType.UNKNOWN

        # Standard properties
        self.authFile = properties.pop("auth", "")
        self.perFileProcessing = properties.pop("perFileProcessing", [])
        self.finalProcessing = properties.pop("finalProcessing", [])
        self.dwcProperties = properties.pop("dwcProperties", {})

        self.locationDir = cfg.folders.datasources / location
        self.databaseDir = self.locationDir / database
        self.systemManager = SystemManager(self.location, self.databaseDir, self.dwcProperties, self.authFile)
        self.timeManager = TimeManager(self.databaseDir)

        self._postInit(properties)
        self.checkLeftovers(properties)

    def __str__(self):
        return f"{self.location}-{self.database}, {self.outputs}"

    def __repr__(self):
        return str(self)

    def _postInit(self, properties: dict) -> None:
        raise NotImplementedError
    
    def _prepare(self, buildProcessing: bool = True, **kwargs) -> None:
        raise NotImplementedError
    
    def prepareStage(self, stage: StageFileStep) -> None:
        buildProcessing = stage != StageFileStep.DOWNLOADED # Do not build processing if stage file step is only for downloaded
        logger.info(f"Preparing database with{'out' if not buildProcessing else ''} processing")

        self._prepare(buildProcessing=buildProcessing)

        if stage in (StageFileStep.DOWNLOADED, StageFileStep.PROCESSED):
            return
        
        logger.info("Preparing pre-DWC files")
        self.systemManager.addFinalStage(self.finalProcessing)

        if stage == StageFileStep.PRE_DWC:
            return
        
        logger.info("Preparing DWC file converter")
        self.systemManager.prepareDwC()

    def createStage(self, stage: StageFileStep, fileNumbers: list[int] = [], overwrite: int = 0, **kwargs: dict) -> None:
        success = self.systemManager.create(stage, fileNumbers, overwrite, **kwargs)
        if success:
            self.timeManager.update(stage)

    def createDirectory(self) -> None:
        Logger.info(f"Creating directory for data: {str(self.databaseDir)}")
        self.databaseDir.mkdir(parents=True, exist_ok=True)

    def checkLeftovers(self, properties: dict) -> None:
        for property in properties:
            Logger.debug(f"{self.location}-{self.database} unknown property: {property}")

    def getDBType(self) -> str:
        return self.dbType
    
    def getBaseDir(self) -> Path:
        return self.databaseDir
    
    def getDownloadedFiles(self, selectIndexes: list[int] = []) -> list[StageFile]:
        return self._selectFiles(StageFileStep.DOWNLOADED, selectIndexes)
    
    def getPreDWCFiles(self, selectIndexes: list[int] = []) -> list[StageFile]:
        return self._selectFiles(StageFileStep.PRE_DWC, selectIndexes)
    
    def getDWCFiles(self, selectIndexes: list[int] = []) -> list[StageFile]:
        return self._selectFiles(StageFileStep.DWC, selectIndexes)

    def _selectFiles(self, stage: StageFileStep, indexes: list[int]) -> list[StageFile]:
        fileList = self.systemManager.getFiles(stage)

        if not indexes:
            return fileList
        
        selectedFiles = []
        invalidIndexes = []
        for index in indexes:
            if index >= 0 or index < len(fileList):
                selectedFiles.append(fileList[index])
            else:
                invalidIndexes.append(index)

        Logger.error(f"Invalid {stage.name} files selected: {invalidIndexes}")
        return selectedFiles

class SpecificDB(Database):

    def _postInit(self, properties: dict) -> None:
        self.dbType = DBType.SPECIFIC
        self.files = properties.pop("files", None)

        if self.files is None:
            raise Exception("No provided files for source") from AttributeError

    def _prepare(self, buildProcessing: bool = True) -> None:
        for file in self.files:
            url = file.get("url", None)
            fileName = file.get("downloadedFile", None)
            processingSteps = file.get("processing", [])
            fileProperties = file.get("fileProperties", {})

            if url is None:
                raise Exception("No url provided for source") from AttributeError

            if fileName is None:
                raise Exception("No filename provided to download to") from AttributeError
            
            self.systemManager.addDownloadURLStage(url, fileName, processingSteps, fileProperties, buildProcessing=buildProcessing)

class LocationDB(Database):

    def _postInit(self, properties: dict) -> None:
        self.dbType = DBType.LOCATION
        self.localFile = "files.txt"

        self.fileProperties = properties.pop("fileProperties", {})
        self.fileLocation = properties.pop("dataLocation", None)
        self.downloadLink = properties.pop("downloadLink", "")
        self.regexMatch = properties.pop("regexMatch", ".*")
        self.maxSubDirDepth = properties.pop("subDirectoryDepth", -1)
        self.folderPrefix = properties.pop("folderPrefix", False)

        if self.fileLocation is None:
            raise Exception("No file location for source") from AttributeError
        
        self.crawler = Crawler(self.databaseDir, self.regexMatch, self.downloadLink, self.maxSubDirDepth, user=self.systemManager.user, password=self.systemManager.password)

    def _prepare(self, buildProcessing: bool = True, recrawl: bool = False) -> None:
        localFilePath = self.databaseDir / self.localFile

        if not recrawl and localFilePath.exists():
            logger.info("Local file found, skipping crawling")
            with open(localFilePath) as fp:
                urls = fp.read().splitlines()
        else:
            localFilePath.unlink(True)
            
            logger.info("Crawling...")
            self.crawler.crawl(self.fileLocation)
            urls = self.crawler.getURLList()

            self.databaseDir.mkdir(parents=True, exist_ok=True) # Create base directory if it doesn't exist to put the file
            with open(localFilePath, 'w') as fp:
                fp.write("\n".join(urls))

        for url in urls:
            fileName = self.getFileNameFromURL(url)
            self.systemManager.addDownloadURLStage(url, fileName, self.perFileProcessing, self.fileProperties, buildProcessing=buildProcessing)

    def getFileNameFromURL(self, url: str) -> str:
        urlParts = url.split('/')
        fileName = urlParts[-1]

        if not self.folderPrefix:
            return fileName

        folderName = urlParts[-2]
        return f"{folderName}_{fileName}"

class ScriptDB(Database):

    def _postInit(self, properties: dict) -> None:
        self.dbType = DBType.SCRIPTDATA
        self.script = properties.pop("script", None)

        if self.script is None:
            raise Exception("No script specified") from AttributeError
        
    def _prepare(self, buildProcessing: bool = True) -> None:
        self.systemManager.addRetrieveScriptStage(self.script, self.perFileProcessing, buildProcessing=buildProcessing)
