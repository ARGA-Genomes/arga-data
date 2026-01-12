import json
from lib.settings import dataSourcesDir
from pathlib import Path
from lib.data.database import Database
import logging

class SourceManager:

    _divider = "-"

    def __init__(self):
        self.locations: dict[str, Location] = {}

        dataSourcesFolder: Path = dataSourcesDir
        for locationFolder in dataSourcesFolder.iterdir():
            if locationFolder.is_file():
                continue

            location = Location(locationFolder)
            self.locations[location.getName()] = location

    def _buildSourceName(self, locationName: str, databaseName: str, subsectionName: str) -> str:
        return f"{locationName}{self._divider}{databaseName}" + (f"{self._divider}{subsectionName}" if subsectionName else "")

    def _splitSourceName(self, sourceName: str) -> tuple[str, str, str]:
        sourceSections = sourceName.split(self._divider, 2)

        if len(sourceSections) <= 1:
            return sourceName, "", ""

        if len(sourceSections) == 2:
            return *sourceSections, ""
        
        if len(sourceSections) == 3:
            return tuple(sourceSections)
        
        raise Exception("Too many sections in source name, should not contain more than 3.")

    def matchSources(self, sourceHint: str = "") -> dict[str, dict[str, list[str]]]:
        locationName, databaseName, subsection = self._splitSourceName(sourceHint)
        if not locationName:
            return {locationName: location.getDatabases() for locationName, location in self.locations.items()}
        
        location = self.locations.get(locationName, None)
        if location is None:
            logging.error(f"Invalid location '{locationName}'")
            return []
        
        return {locationName: location.getDatabases(databaseName, subsection)}

    def countSources(self, sources: dict[str, dict[str, list[str]]]) -> int:
        return sum(1 for _, databases in sources.items() for _, subsections in databases.items() for _ in subsections)

    def constructDBs(self, sources: dict[str, dict[str, list[str]]]) -> list[Database]:
        dbs = []
        for locationName, databases in sources.items():
            for databaseName, subsections in databases.items():
                for subsection in subsections:
                    name = self._buildSourceName(locationName, databaseName, subsection)

                    logging.info(f"Constructing database: {name}")
                    location = self.locations[locationName]
                    constructedDB = location.constructDB(databaseName, subsection, name)

                    if constructedDB is None:
                        continue

                    dbs.append(constructedDB)

        return dbs

class Location:
    def __init__(self, locationPath: Path):
        self.locationPath = locationPath
        self.databases: dict[str, Database] = {}

        for databaseFolder in locationPath.iterdir():
            if databaseFolder.is_file() or databaseFolder.name in ("__pycache__", "llib"): # Skip files, cached python folder, and location library
                continue

            db = Database(databaseFolder)
            self.databases[db.databaseName()] = db

    def getName(self) -> str:
        return self.locationPath.name
    
    def getDatabases(self, databaseName: str = "", subsectionName: str = "") -> dict[str, list[str]]:
        noSubsections = [""]

        if not databaseName:
            dbs = {}
            for databaseName, database in self.databases.items():
                databaseSubsections = database.listSubsections()
                if not databaseSubsections:
                    databaseSubsections = noSubsections

                dbs[databaseName] = databaseSubsections

            return dbs
        
        database = self.databases.get(databaseName, None)
        if database is None:
            logging.error(f"Invalid database '{databaseName}' for location '{self.getName()}'")
            return {}
        
        databaseSubsections = database.listSubsections()
        if not subsectionName:
            if not databaseSubsections:
                databaseSubsections = noSubsections

            return {databaseName: databaseSubsections}

        if subsectionName not in databaseSubsections:
            logging.error(f"No subsection '{subsectionName}' exists under database '{databaseName}' for location '{self.getName()}'")
            return {}

        return {databaseName: [subsectionName]}
    
    def constructDB(self, databaseName, subsection: str, name: str) -> Database:
        database = self.databases[databaseName]
        database.constuct(name, subsection)
        return database
