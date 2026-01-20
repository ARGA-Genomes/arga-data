import numpy as np
import pandas as pd
import urllib.error
import json
from pathlib import Path
import logging
import lib.common as cmn

class Map:
    _unmappedLabel = "unmapped"

    def __init__(self, translationData: dict[str, tuple[str, str, list[str]]]):
        self.translation = translationData
        self.events = []

        for originalName, mappingData in translationData.items():
            event, mappedName, fallbacks = mappingData
            if event not in self.events:
                self.events.append(event)

        self.events.append(self._unmappedLabel)

    @classmethod
    def fromFile(cls, filePath: Path) -> 'Map':
        if not filePath.exists():
            logging.warning(f"No map file found at path: {filePath}")
            return cls({})
        
        with open(filePath) as fp:
            return cls(json.load(fp))
    
    @classmethod
    def fromSheets(cls, sheetID: int, saveFilePath: Path = None) -> 'Map':
        documentID = "1dglYhHylG5_YvpslwuRWOigbF5qhU-uim11t_EE_cYE"
        df = cls._loadGoogleSheet(documentID, sheetID)
        
        if df is None:
            return cls({})
        
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

        fields = "Field Name"
        eventColumns = [col for col in df.columns if col[0] == "T" and col[1].isdigit()]

        mapping = {}
        for column, eventName in zip(eventColumns, eventNames):
            subDF = df[[fields, column]] # Select only the dwc name and event columns

            for _, row in subDF.iterrows():
                mappedName = cmn.toSnakeCase(row[fields])
                oldNamesCell = row[column]

                if not oldNamesCell:
                    continue

                if not isinstance(oldNamesCell, str): # Ignore float/int
                    continue

                if oldNamesCell in ("", "0", "1", "nan", "NaN", np.nan, np.NaN, np.NAN): # Ignore these values
                    continue

                if any(oldNamesCell.startswith(prefix) for prefix in ("ARGA", '"', "/")): # Ignore values with these prefixes
                    continue

                # Remove sections in braces
                openBrace = oldNamesCell.find("(")
                closeBrace = oldNamesCell.rfind(")", openBrace)
                if openBrace >= 0 and closeBrace >= 0:
                    oldNamesCell = oldNamesCell[:openBrace] + oldNamesCell[closeBrace+1:]

                oldNames = [subname.split("::")[0].strip(" :") for subname in oldNamesCell.split(",")] # Overwrite old name with list of subnames
                mapping[oldNames[0]] = (eventName, mappedName, oldNames[1:])

        instance = cls(mapping)
        if saveFilePath is not None:
            instance.save(saveFilePath)

        return instance
    
    @classmethod
    def fromModernSheet(cls, columnName: str, saveFilePath: Path = None) -> 'Map':
        documentID = "1XBQ8Hz_MWM8LCvr379AO73zFGzn3dqQMrGi8U-95FZ0"
        df = cls._loadGoogleSheet(documentID, 0)

        if df is None:
            return cls({})
        
        event = "event"
        argaSchema = "arga_schema_label"
        df = df[[event, argaSchema, columnName]]

        mapping = {}
        for _, row in df.iterrows():
            oldNamesCell = row[columnName]

            if not oldNamesCell:
                continue

            if not isinstance(oldNamesCell, str): # Ignore float/int
                continue

            if oldNamesCell.startswith('"'):
                continue

            if not row[event]:
                continue

            oldNames = [subName.strip() for subName in oldNamesCell.split(";")]
            mapping[oldNames[0]] = (row[event], row[argaSchema], oldNames[1:])

        instance = cls(mapping)
        if saveFilePath is not None:
            instance.save(saveFilePath)

        return instance

    @staticmethod
    def _loadGoogleSheet(documentID: str, sheetID: int) -> pd.DataFrame:
        webURL = f"https://docs.google.com/spreadsheets/d/{documentID}/edit?gid={sheetID}#gid={sheetID}"
        retrieveURL = f"https://docs.google.com/spreadsheets/d/{documentID}/export?format=csv&gid={sheetID}"

        logging.info(f"Reading sheet {retrieveURL}")
        try:
            return pd.read_csv(retrieveURL, keep_default_na=False)
        except urllib.error.HTTPError:
            logging.warning(f"Unable to read sheet. Web URL: {webURL}")
            return None
        
    def save(self, filePath: Path) -> None:
        with open(filePath, "w") as fp:
            json.dump(self.translation, fp, indent=4)

        logging.info(f"Saved map data to local file: {filePath}")

    def applyTo(self, df: pd.DataFrame, unmappedPrefix: str = "") -> dict[str, pd.DataFrame]:
        eventCollections: dict[str, list[pd.Series]] = {}

        for column in df.columns:
            event, newName, fallbacks = self.translation.get(column, (self._unmappedLabel, f"{unmappedPrefix}{'_' if unmappedPrefix else ''}{column}", []))

            if event not in eventCollections:
                eventCollections[event] = []

            series = df[column]
            for fallback in fallbacks:
                if fallback in df.columns:
                    series = series.fillna(df[fallback])

            eventCollections[event].append(series.rename(newName))

        for event, seriesList in eventCollections.items():
            eventCollections[event] = pd.concat(seriesList, axis=1) # Convert list of series to dataframe

        return {event: pd.concat(seriesList, axis=1) for event, seriesList in eventCollections.items()}

    def isEmpty(self) -> bool:
        return not self.translation
