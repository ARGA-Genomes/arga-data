import numpy as np
import pandas as pd
import urllib.error
import json
from pathlib import Path
import logging
import lib.common as cmn

class Map:
    def __init__(self, data: dict[str, tuple[str, str]] = {}):
        self.data: dict[str, tuple[str, str]] = {}
        self.events: list[str] = []

        for originalName, eventMapping in data.items():
            event, newName = eventMapping
            self.addMapping(originalName, event, newName)

    def save(self, filePath: Path):
        with open(filePath, "w") as fp:
            json.dump(self.data, fp, indent=4)

        logging.info(f"Saved map data to local file: {filePath}")

    def addMapping(self, originalName: str, event: str, newName: str) -> None:
        self.data[originalName] = (event, newName)

        if event not in self._events:
            self.events.append(event)

def loadFromFile(filePath: Path) -> Map:
    if not filePath.exists():
        logging.warning(f"No map file found at path: {filePath}")
        return {}
    
    with open(filePath) as fp:
        return Map(json.load(fp))

def loadFromSheets(sheetID: int, localSavePath: Path = None) -> Map:
    documentID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"
    retrieveURL = f"https://docs.google.com/spreadsheets/d/{documentID}/export?format=csv&gid={sheetID}"
    eventNames = [
        "collection",
        "accession",
        "sample prep",
        "extraction",
        "sequencing",
        "assembly",
        "annotation",
        "record level"
    ]

    try:
        df = pd.read_csv(retrieveURL, keep_default_na=False)
    except urllib.error.HTTPError:
        logging.warning(f"Unable to read sheet with id: {sheetID}")
        return {}

    fields = "Field Name"
    eventColumns = [col for col in df.columns if col[0] == "T" and col[1].isdigit()]
    map = Map()

    for column, eventName in zip(eventColumns, eventNames):
        subDF = df[[fields, column]] # Select only the dwc name and event columns

        for _, row in subDF.iterrows():
            mappedName = cmn.toSnakeCase(row[fields])
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
            map.addMapping(oldName, eventName, mappedName)

    if localSavePath is not None:
        map.save(localSavePath)

    return map

def loadFromModernSheet(columnName: str, localSavePath: Path = None) -> Map:
    return {}

def applyMap(map: Map, df: pd.DataFrame, unmappedPrefix: str = "") -> pd.DataFrame:
    eventMapping: dict[str, dict[str, str]] = {}

    for column in df.columns:
        event, newName = map.data.get(column, ("unmapped", f"{unmappedPrefix}{'_' if unmappedPrefix else ''}{column}"))

        if event not in eventMapping:
            eventMapping[event] = {}

        eventMapping[event][column] = newName

    for event, columnMapping in eventMapping.items():
        subDF: pd.DataFrame = df[columnMapping.keys()].copy() # Select only relevant columns
        eventMapping[event] = subDF.rename(columnMapping, axis=1)

    return pd.concat(eventMapping.values(), keys=eventMapping.keys(), axis=1)

class RepeatRemapper:
    def __init__(self, map: Map, unmappedPrefix: str = ""):
        self.map = map
        self.unmappedPrefix = unmappedPrefix

    def applyMapping(self, df: pd.DataFrame) -> pd.DataFrame:
        return applyMap(self.map, df, self.unmappedPrefix)
