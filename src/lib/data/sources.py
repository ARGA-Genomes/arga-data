import json
from lib.config import globalConfig as gcfg
from pathlib import Path
from lib.data.database import BasicDB, CrawlDB, ScriptDB, Retrieve
import logging

class SourceManager:
    def __init__(self):
        self.locations: dict[str, Location] = {}
        
        for locationPath in gcfg.folders.dataSources.iterdir():
            locationObj = Location(locationPath)
            self.locations[locationObj.locationName] = locationObj
    
    def getLocations(self) -> dict[str, 'Location']:
        return self.locations

    def requestDBs(self, source: str) -> list[BasicDB]:
        sourceInformation = source.split("-")

        if len(sourceInformation) >= 4:
            logging.error(f"Unknown source: {source}")
            return []
        
        locationStr, databaseStr, subsectionStr = (sourceInformation + ["", ""])[:3] # Force sourceInformation to be 3 long

        location = self.locations.get(locationStr, None)
        if location is None:
            logging.error(f"Invalid location: {locationStr}")
            return []
        
        return location.loadDBs(databaseStr, subsectionStr)

class Location:
    def __init__(self, locationPath: Path):
        self.locationPath = locationPath
        self.locationName = locationPath.stem

        # Setup databases
        self.databases: dict[str, Database] = {}
        for databaseFolder in locationPath.iterdir():
            if databaseFolder.is_file() or databaseFolder.name == "__pycache__": # Skip files and cached python folder
                continue

            self.databases[databaseFolder.stem] = Database(self.locationName, databaseFolder)

    def getDatabases(self) -> dict[str, 'Database']:
        return self.databases

    def loadDBs(self, database: str, subsection: str) -> list[BasicDB]:
        constructDBs = []
        if database:
            if database not in self.databases:
                logging.error(f"Invalid database: {database}")
                return []
            constructDBs.append(database)
        else: # Load all dbs if database is empty string
            constructDBs.extend(self.databases.keys())

        dbs = []
        for dbName in constructDBs:
            databaseObject = self.databases[dbName]
            dbs.extend(databaseObject.constructDBs(subsection))

        return dbs
        
class Database:
    configFile = "config.json"
    dbMapping = {
        Retrieve.URL: BasicDB,
        Retrieve.CRAWL: CrawlDB,
        Retrieve.SCRIPT: ScriptDB
    }

    def __init__(self, locationName: str, databasePath: Path):
        self.locationName = locationName
        self.databasePath = databasePath
        self.databaseName = databasePath.stem

    def _loadConfig(self) -> dict | None:
        configPath = self.databasePath / self.configFile
        if not configPath.exists():
            logging.error(f"No config file found for database '{self.locationName}-{self.databaseName}'")
            return None
        
        with open(configPath) as fp:
            return json.load(fp)
    
    def _translateSubsection(self, config: dict, subsectionName: str, subsectionProperties: dict) -> dict:

        def translate(obj: any) -> any:
            if isinstance(obj, str):
                obj = obj.replace("{SUBSECTION}", subsectionName)
                for key, value in subsectionProperties.items():
                    obj = obj.replace(f"{{SUBSECTION:{key.upper()}}}", value)

                return obj
            
            if isinstance(obj, list):
                return [translate(item) for item in obj]
            
            if isinstance(obj, dict):
                return {key: translate(value) for key, value in obj.items()}
            
            return obj

        return {key: translate(value) for key, value in config.items()}
        
    def constructDBs(self, subsection: str) -> list[BasicDB]:
        databaseConfig = self._loadConfig()
        if databaseConfig is None:
            return []
        
        retrieveType = databaseConfig.pop("retrieveType", None)
        if retrieveType is None:
            logging.error(f"No retrieve type specified for database '{self.locationName}-{self.databaseName}'")
            return []
        
        retrieveType = Retrieve(retrieveType)
        dbType = self.dbMapping.get(retrieveType, None)
        if dbType is None:
            logging.error(f"Database {self.databaseName} has invalid retrieve type: {retrieveType.value}. Should be one of: {', '.join(key.value for key in self.dbMapping)}")
            return []
        
        # Determine which subsections to load
        subsections = databaseConfig.pop("subsections", {})
        loadSubsections = {}
        if subsections: # Subsections in config
            if subsection: # User defined subsection
                if subsection not in subsections:
                    logging.error(f"Invalid subsection: {subsection}")
                    return []
                else: # Valid subsection provided
                    loadSubsections[subsection] = subsections[subsection]
            else: # No subsection provided, collect all
                loadSubsections = subsections
        else: # No subsection provided and none exist, no need to translate
            loadSubsections = {"": {}}

        # Derive configs for each subsection and check for dataset ID
        configs = {}
        for subsectionName, subsectionProperties in loadSubsections.items():
            config = databaseConfig if not subsectionName else self._translateSubsection(databaseConfig, subsectionName, subsectionProperties)

            datasetID = config.pop("datasetID", None)
            if datasetID is None:
                error = f"No datasetID specified for database '{self.locationName}-{self.databaseName}'"
                if subsectionName:
                    error += f" with subsection '{subsectionName}'"
                error += f" - Conversion process will not work."

                logging.warning(error)

            configs[subsectionName] = (datasetID, config)
        
        dbs = []
        for subsectionName, configData in configs.items():
            datasetID, config = configData
            databaseName = f"{self.locationName}-{self.databaseName}{f'-{subsectionName}' if subsectionName else ''}"

            try:
                logging.info(f"Creating database '{databaseName}'")
                dbs.append(dbType(self.locationName, self.databaseName, subsectionName, datasetID, config))
            except AttributeError as e:
                logging.error(f"Error creating database '{databaseName}' - {e}")
                continue

        return dbs
