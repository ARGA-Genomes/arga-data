import numpy as np
import pandas as pd
import urllib.error
import json
import lib.config as cfg
import lib.commonFuncs as cmn
from pathlib import Path
from enum import Enum
from lib.tools.logger import logger
from dataclasses import dataclass

class Events(Enum):
    COLLECTIONS = "collections"
    ACCESSIONS = "accessions"
    SUBSAMPLES = "subsamples"
    EXTRACTIONS = "dna_extractions"
    SEQUENCES = "sequences"
    ASSEMBLIES = "assemblies"
    ANNOTATIONS = "annotations"
    DEPOSITIONS = "depositions"

@dataclass(eq=True, frozen=True)
class MappedColumn:
    event: str
    colName: str

class Remapper:
    def __init__(self, location: str, customMapPath: Path = None, preserveDwCMatch: bool = False, prefixMissing: bool = True) -> 'Remapper':
        self.location = location
        self.customMapPath = customMapPath
        self.preserveDwCMatch = preserveDwCMatch
        self.prefixMissing = prefixMissing

        self.mappedColumns: dict[str, list[MappedColumn]] = {}
        self.uniqueColumns: dict[MappedColumn, list[str]] = {}
        self._loadMaps(customMapPath)

    def getMappings(self) -> dict:
        return self.mappedColumns

    def _loadMaps(self, customMapPath: Path = None) -> None:
        # DWC map
        mapPath = cfg.folders.mapping / f"{self.location}.json"
        if mapPath.exists():
            self.map = cmn.loadFromJson(mapPath)
        else:
            logger.warning(f"No DWC map found for location {self.location}")
            self.map = {}
        
        self.lookup = self._buildLookup(self.map)

        # Exit early if no custom path specified
        if customMapPath is None:
            self.customLookup = {}
            return

        # Custom map
        if customMapPath.exists():
            self.customMap = cmn.loadFromJson(customMapPath)
        else:
            raise Exception(f"No custom map found at location: {customMapPath}") from FileNotFoundError

        self.customLookup = self._buildLookup(self.customMap)

    def _buildLookup(self, map: dict[str, dict[str, list[str]]]) -> dict[str, list[MappedColumn]]:
        lookup: dict[str, list[MappedColumn]] = {}

        for eventName, columnMap in map.items():
            for newName, oldNameList in columnMap.items():
                for oldName in oldNameList:
                    if oldName not in lookup:
                        lookup[oldName] = []
  
                    lookup[oldName].append(MappedColumn(eventName, newName))

        return lookup
    
    def createMappings(self, columns: list[str], skipRemap: list[str] = []) -> dict[str, list[MappedColumn]]:
        self.mappedColumns.clear()
        self.uniqueColumns.clear()

        for column in columns:
            if column in skipRemap:
                continue

            self.mappedColumns[column] = []

            # Apply DWC mapping
            for lookup in (self.lookup, self.customLookup):
                lookupValues = lookup.get(column, [])
                self.mappedColumns[column].extend(lookupValues)
                for mapping in lookupValues:
                    if mapping not in self.uniqueColumns:
                        self.uniqueColumns[mapping] = []
                    
                    self.uniqueColumns[mapping].append(column)

            # If column matches an output column name
            if column in self.map and self.preserveDwCMatch:
                mapValue = MappedColumn("Preserved", f"{self.location}_{column}")
                self.mappedColumns[column].append(mapValue)

                if mapValue not in self.uniqueColumns:
                    self.uniqueColumns[mapValue] = []
                    
                self.uniqueColumns[mapValue].append(column)

            # If no mapped value has been found yet
            if not self.mappedColumns[column]:
                mapValue = MappedColumn("Unmapped", f"{self.location}_{column}" if self.prefixMissing else column)
                self.mappedColumns[column].append(mapValue)

                if mapValue not in self.uniqueColumns:
                    self.uniqueColumns[mapValue] = []
                    
                self.uniqueColumns[mapValue].append(column)

        return self.mappedColumns
    
    def allUnique(self) -> bool:
        return all(len(originalColumns) == 1 for originalColumns in self.uniqueColumns.values())

    def getEvents(self) -> list[str]:
        return list(set([mapping.event for mapList in self.mappedColumns.values() for mapping in mapList]))
    
    def reportMatches(self) -> None:
        for mapping, oldColumns in self.uniqueColumns.items():
            initialMapping = oldColumns[0]

            for column in oldColumns[1:]:
                logger.info(f"Found mapping for column '{column}' that matches initial mapping '{initialMapping}' with event '{mapping.event}'")

    def forceUnique(self) -> None:
        self.mappedColumns.clear()

        for mapping, oldColumns in self.uniqueColumns.items():
            self.mappedColumns[oldColumns[0]] = [mapping]

    def applyMap(self, df: pd.DataFrame) -> pd.DataFrame:
        eventColumns = {}

        for column in df.columns:
            for mappedColumn in self.mappedColumns[column]:
                if mappedColumn.event not in eventColumns:
                    eventColumns[mappedColumn.event] = {}

                eventColumns[mappedColumn.event][column] = mappedColumn.colName

        for eventName, colMap in eventColumns.items():
            subDF: pd.DataFrame = df[colMap.keys()].copy() # Select only relevant columns
            subDF.rename(colMap, axis=1, inplace=True)
            eventColumns[eventName] = subDF

        return pd.concat(eventColumns.values(), keys=eventColumns.keys(), axis=1)

class MapRetriever:
    def __init__(self):
        self.documentID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"
        self.retrieveURL = f"https://docs.google.com/spreadsheets/d/{self.documentID}/export?format=csv&gid="
        self.sheetIDs = {
            "42bp-genomeArk": 84855374,
            "ala-avh": 404635334,
            "anemone-db": 286004534,
            "bold-datapackage": 1154592624,
            "bold-tsv": 78385490,
            "bold-xml": 984983691,
            "bpa-portal": 1982878906,
            "bvbrc-db": 685936034,
            "csiro-dap": 16336602,
            "csiro-genomes": 215504073,
            "dnazoo-db": 570069681,
            "ena-genomes": 1058330275,
            "ncbi-biosample": 109194600,
            "ncbi-nucleotide": 1759032197,
            "ncbi-refseq": 2003682060,
            "ncbi-taxonomy": 240630744,
            "ncbi-genbank": 1632006425,
            "ncbi-assemblyStats": 1145358378,
            "tern-portal": 1651969444,
            "tsi-koala": 975794491
        }

    def run(self) -> None:
        written = []
        for database, sheetID in self.sheetIDs.items():
            location, _ = database.split("-")
            logger.debug(f"Reading {database}")

            try:
                df = pd.read_csv(self.retrieveURL + str(sheetID), keep_default_na=False)
            except urllib.error.HTTPError:
                logger.warning(f"Unable to read sheet for {database}")
                continue

            mappings = self.getMappings(df)
            mapFile = cfg.folders.mapping / f"{location}.json"

            if mapFile.exists() and location not in written: # Old map file
                mapFile.unlink()

            if not mapFile.exists(): # First data source from location
                with open(mapFile, "w") as fp:
                    json.dump(mappings, fp, indent=4)
                written.append(location)
                logger.debug(f"Created new {location} map")
                continue

            with open(mapFile) as fp:
                columnMap = json.load(fp)

            for eventName, dwcMap in mappings.items():
                for keyword, names in dwcMap.items():
                    if keyword not in columnMap[eventName]:
                        columnMap[eventName][keyword] = names
                    else:
                        columnMap[eventName][keyword].extend(name for name in names if name not in columnMap[eventName][keyword])

            with open(mapFile, "w") as fp:
                json.dump(columnMap, fp, indent=4)

            logger.debug(f"Added new values to {location} map")
            if location not in written:
                written.append(location)

    def getMappings(self, df: pd.DataFrame) -> dict[str, dict]:
        fields = "Field Name"
        eventColumns = [col for col in df.columns if col[0] == "T" and col[1].isdigit()]
        mappings = {event.value: {} for event in Events}

        for column, eventName in zip(eventColumns, mappings.keys()):
            subDF = df[[fields, column]]
            for _, row in subDF.iterrows():
                filteredValues = self.filterEntry(row[column])
                if filteredValues:
                    mappings[eventName][row[fields]] = filteredValues

        return mappings
    
    def filterEntry(self, value: any) -> list:
        if not isinstance(value, str): # Ignore float/int
            return []
        
        if value in ("", "0", "1", "nan", "NaN", np.nan, np.NaN, np.NAN): # Ignore these values
            return []
        
        if any(value.startswith(val) for val in ("ARGA", '"', "/")): # Ignore values with these prefixes
            return []

        return [elem.strip() for elem in value.split(",")]
