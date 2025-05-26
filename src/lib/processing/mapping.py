from __future__ import annotations
import numpy as np
import pandas as pd
import urllib.error
import json
from pathlib import Path
from enum import Enum
import logging
from dataclasses import dataclass
import lib.common as cmn

class Event(Enum):
    COLLECTION = "collections"
    ACCESSION = "accessions"
    SUBSAMPLE = "subsamples"
    EXTRACTION = "dna_extractions"
    SEQUENCE = "sequences"
    ASSEMBLIES = "assemblies"
    ANNOTATION = "annotations"
    DEPOSITION = "depositions"
    UNMAPPED = "unmapped"
    PRESERVED = "preserved"

@dataclass(frozen=True, eq=True)
class MappedColumn:
    event: Event
    colName: str

class Map:
    def __init__(self, mappings: dict = {}) -> Map:
        self._mappings = mappings

        if mappings:
            self._lookup = self._reverseLookup(mappings)
        else:
            self._lookup = {}

    @classmethod
    def fromFile(cls, filePath: Path) -> Map:
        if not filePath.exists():
            logging.warning(f"No DWC map found at path: {filePath}")
            return cls()
        
        with open(filePath) as fp:
            rawMap = json.load(fp)

        mappings = {}
        for event, dwcMap in rawMap.items():
            if event not in Event._value2member_map_:
                logging.warning(f"Unknown event: {event}, skipping")
                continue

            mappings[Event(event)] = dwcMap

        return cls(mappings)

    @classmethod
    def fromSheets(cls, sheetID: int) -> Map:
        documentID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"
        retrieveURL = f"https://docs.google.com/spreadsheets/d/{documentID}/export?format=csv&gid={sheetID}"

        try:
            df = pd.read_csv(retrieveURL, keep_default_na=False)
        except urllib.error.HTTPError:
            logging.warning(f"Unable to read sheet with id: {sheetID}")
            return cls()

        fields = "Field Name"
        eventColumns = [col for col in df.columns if col[0] == "T" and col[1].isdigit()]
        mappings = {event: {} for event in Event}

        for column, event in zip(eventColumns, mappings.keys()):
            subDF = df[[fields, column]] # Select only the dwc name and event columns
            for _, row in subDF.iterrows():
                dwcName = cmn.toSnakeCase(row[fields])
                oldName = row[column]

                # Clean the old name cell
                if not oldName:
                    continue

                if not isinstance(oldName, str): # Ignore float/int
                    continue

                if oldName in ("", "0", "1", "nan", "NaN", np.nan, np.NaN, np.NAN): # Ignore these values
                    continue

                if any(oldName.startswith(prefix) for prefix in ("ARGA", '"', "/")): # Ignore values with these prefixes
                    continue

                # Remove sections in braces
                openBrace = oldName.find("(")
                closeBrace = oldName.rfind(")", openBrace)
                if openBrace >= 0 and closeBrace >= 0:
                    oldName = oldName[:openBrace] + oldName[closeBrace+1:]

                oldName = [subname.split("::")[-1].strip(" :") for subname in oldName.split(",")] # Overwrite old name with list of subnames
                mappings[Event(event)][dwcName] = oldName

        return cls(mappings)
    
    def hasMappings(self) -> bool:
        return len(self._mappings) > 0

    def saveToFile(self, filePath: Path) -> None:
        output = {}
        for event, dwcMap in self._mappings.items():
            output[event.value] = dwcMap

        with open(filePath, "w") as fp:
            json.dump(output, fp, indent=4)

    def getValues(self, fieldName: str) -> list[MappedColumn]:
        return self._lookup.get(fieldName, [])
    
    def existsInMap(self, fieldName: str) -> bool:
        return fieldName in self._mappings

    @staticmethod
    def _reverseLookup(map: dict[Event, dict[str, list[str]]]) -> dict[str, list[MappedColumn]]:
        lookup: dict[str, list[MappedColumn]] = {}

        for event, columnMap in map.items():
            for newName, oldNameList in columnMap.items():
                for oldName in oldNameList:
                    if oldName not in lookup:
                        lookup[oldName] = []
  
                    lookup[oldName].append(MappedColumn(event, newName))

        return lookup
    
class TranslationTable:
    def __init__(self):
        self._translationTable: dict[str, list[MappedColumn]] = {}
        self._uniqueEntries: dict[MappedColumn, list[str]] = {}
        self._unmappedColumns: list[str] = []

        self._eventsUsed = set()

    def clear(self) -> None:
        self._translationTable.clear()
        self._uniqueEntries.clear()
        self._unmappedColumns.clear()

    def addTranslation(self, column: str, columnMapping: MappedColumn) -> None:
        if column not in self._translationTable:
            self._translationTable[column] = []

        self._translationTable[column].append(columnMapping)
        self._eventsUsed.add(columnMapping.event)

        if columnMapping.event == Event.UNMAPPED:
            self._unmappedColumns.append(columnMapping.colName)

        if columnMapping not in self._uniqueEntries:
            self._uniqueEntries[columnMapping] = []
        
        self._uniqueEntries[columnMapping].append(column)

    def getTranslation(self, column: str) -> list[MappedColumn]:
        return self._translationTable.get(column, [])

    def getEventCategories(self) -> list[Event]:
        return list(self._eventsUsed)
    
    def getUnmapped(self) -> list[str]:
        return self._unmappedColumns

    def hasColumn(self, column: str) -> bool:
        return column in self._translationTable
    
    def allUniqueColumns(self) -> bool:
        return all(len(originalColumns) == 1 for originalColumns in self._uniqueEntries.values())
    
    def getNonUnique(self) -> list[tuple[Event, str, list[str]]]:
        return [(mapping.event, oldCols[0], oldCols[1:]) for mapping, oldCols in self._uniqueEntries.items()]
    
    def forceUnique(self) -> None:
        for mapping, oldColumns in self._uniqueEntries.items():
            for column in oldColumns[1:]:
                self._translationTable[column].remove(mapping)

class Remapper:
    def __init__(self, baseDir: Path, mapID: int, customMapID: int, customMapPath: Path, prefix: str, preserveDwC: bool = False, prefixUnmapped: bool = True):
        self.baseDir = baseDir
        self.localMapPath = baseDir / "map.json"
        self.mapID = mapID
        self.customMapID = customMapID
        self.customMapPath = customMapPath
        self.prefix = prefix
        self.preserveDwc = preserveDwC
        self.prefixUnmapped = prefixUnmapped

        self.table = None

    def _loadMaps(self, forceRetrieve: bool = False) -> list[Map]:
        maps = []

        dwcMap = Map.fromFile(self.localMapPath)
        if not dwcMap.hasMappings() or forceRetrieve:
            dwcMap = Map.fromSheets(self.mapID)

            if dwcMap.hasMappings():
                logging.info("Added sheets map")
                maps.append(dwcMap)
                dwcMap.saveToFile(self.localMapPath)
        else:
            logging.info("Added local map")
            maps.append(dwcMap)
        
        if self.customMapPath is not None:
            customMap = Map.fromFile(self.customMapPath)

            if not customMap.hasMappings():
                if self.customMapID is not None:
                    customMap = Map.fromSheets(self.customMapID)

                    if customMap.hasMappings():
                        logging.info("Added sheets custom map")
                        maps.append(customMap)

                        if self.customMapPath is not None:
                            customMap.saveToFile(self.customMapPath)
            else:
                logging.info("Added local custom map")
                maps.append(customMap)

        return maps

    def buildTable(self, columns: list[str], skipRemap: list[str] = [], forceRetrieve: bool = False) -> bool:
        def buildUnmapped(column: str) -> MappedColumn:
            return MappedColumn(Event.UNMAPPED, f"{self.prefix}_{column}" if self.prefixUnmapped else column)

        maps = self._loadMaps(forceRetrieve)
        if not maps:
            logging.error("Unable to retrieve any maps")
            return False
        
        table = TranslationTable()

        for column in columns:
            if column in skipRemap:
                table.addTranslation(column, buildUnmapped(column))
                continue
 
            # Apply mapping
            for map in maps:
                for value in map.getValues(column):
                    if not value:
                        continue

                    table.addTranslation(column, value)

                # If column matches an output column name
                if map.existsInMap(column) and self.preserveDwCMatch:
                    value = MappedColumn(Event.PRESERVED, f"{self.prefix}_{column}")
                    table.addTranslation(column, value)

            # If no mapped value has been found yet
            if not table.hasColumn(column):
                table.addTranslation(column, buildUnmapped(column))

        self.table = table
        return True

    def applyTranslation(self, df: pd.DataFrame) -> pd.DataFrame:
        eventColumns = {}
        if self.table is None:
            raise Exception("No table defined, please call buildTable before this method.")

        for column in df.columns:
            for mappedColumn in self.table.getTranslation(column):
                if mappedColumn.event not in eventColumns:
                    eventColumns[mappedColumn.event] = {}

                eventColumns[mappedColumn.event][column] = mappedColumn.colName

        for eventName, colMap in eventColumns.items():
            subDF: pd.DataFrame = df[colMap.keys()].copy() # Select only relevant columns
            eventColumns[eventName] = subDF.rename(colMap, axis=1)

        return pd.concat(eventColumns.values(), keys=eventColumns.keys(), axis=1)
