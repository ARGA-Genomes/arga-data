import pandas as pd
import numpy as np
from pathlib import Path
import lib.commonFuncs as cmn
from lib.tools.bigFileWriter import BigFileWriter
from lib.processing.mapping import Remapper, Event
from lib.processing.stages import File, StackedFile
from lib.processing.scripts import Script
from lib.systemManagers.baseManager import SystemManager, Metadata
from lib.tools.logger import Logger
import gc
import time
from datetime import datetime
import lib.tools.zipping as zp

class ConversionManager(SystemManager):
    def __init__(self, baseDir: Path, converionDir: Path, datasetID: str, location: str, database: str, subsection: str):
        super().__init__(baseDir, "converting", "tasks")

        self.conversionDir = converionDir
        self.location = location
        self.datasetID = datasetID

        self.output = StackedFile(self.conversionDir / (f"{location}-{database}" + (f"-{subsection}" if subsection else "")))

        self.fileLoaded = False

    def loadFile(self, file: File, properties: dict, mapDir: Path) -> None:
        self.file = file

        self.mapID = properties.pop("mapID", -1)
        self.customMapID = properties.pop("customMapID", -1)
        self.customMapPath = properties.pop("customMapPath", None)

        self.chunkSize = properties.pop("chunkSize", 1024)
        self.setNA = properties.pop("setNA", [])
        self.fillNA = ColumnFiller(properties.pop("fillNA", {}))
        self.skipRemap = properties.pop("skipRemap", [])
        self.preserveDwC = properties.pop("preserveDwC", False)
        self.prefixUnmapped = properties.pop("prefixUnmapped", True)
        self.augments = [Script(self.baseDir, self.conversionDir, augProperties, []) for augProperties in properties.pop("augment", [])]

        self.remapper = Remapper(mapDir, self.mapID, self.customMapID, self.customMapPath, self.location, self.preserveDwC, self.prefixUnmapped)
        self.fileLoaded = True

    def convert(self, overwrite: bool = False, verbose: bool = True, ignoreRemapErrors: bool = True, forceRetrieve: bool = False) -> bool:
        if not self.fileLoaded:
            Logger.error("No file loaded for conversion, exiting...")
            return False

        if self.datasetID is None:
            Logger.error("No datasetID provided which is required for conversion, exiting...")
            return False

        if self.output.filePath.exists() and not overwrite:
            Logger.info(f"{self.output.filePath} already exists, exiting...")
            return True
        
        # Get columns and create mappings
        Logger.info("Getting column mappings")
        columns = cmn.getColumns(self.file.filePath, self.file.separator, self.file.firstRow)

        success = self.remapper.buildTable(columns, self.skipRemap, forceRetrieve)
        if not success:
            return False
        
        if not self.remapper.table.allUniqueColumns(): # If there are non unique columns
            if not ignoreRemapErrors:
                for event, firstCol, matchingCols in self.remapper.table.getNonUnique():
                    for col in matchingCols:
                        Logger.warning(f"Found mapping for column '{col}' that matches initial mapping '{firstCol}' under event '{event.value}'")
                return False
            
            self.remapper.table.forceUnique()
        
        Logger.info("Resolving events")
        writers: dict[str, BigFileWriter] = {}
        for event in self.remapper.table.getEventCategories():
            cleanedName = event.value.lower().replace(" ", "_")
            writers[event] = BigFileWriter(self.output.filePath / f"{cleanedName}.csv", f"{cleanedName}_chunks")

        Logger.info("Processing chunks for conversion")

        totalRows = 0
        startTime = time.perf_counter()

        chunks = cmn.chunkGenerator(self.file.filePath, self.chunkSize, self.file.separator, self.file.firstRow, self.file.encoding)
        for idx, df in enumerate(chunks, start=1):
            if verbose:
                print(f"At chunk: {idx}", end='\r')

            df = self.remapper.applyTranslation(df) # Returns a multi-index dataframe
            for na in self.setNA:
                df = df.replace(na, np.NaN)

            df = self.fillNA.apply(df)
            df = self.applyAugments(df)
            df[(Event.COLLECTION, "dataset_id")] = self.datasetID
            df[(Event.COLLECTION, "entity_id")] = df[(Event.COLLECTION, "dataset_id")] + df[(Event.COLLECTION, "scientific_name")]

            for eventColumn in df.columns.levels[0]:
                writers[eventColumn].writeDF(df[eventColumn])

            totalRows += len(df)
            del df
            gc.collect()

        for writer in writers.values():
            writer.oneFile()

        self.updateMetadata(0, {
            Metadata.OUTPUT: self.output.filePath.name,
            Metadata.SUCCESS: True,
            Metadata.DURATION: time.perf_counter() - startTime,
            Metadata.TIMESTAMP: datetime.now().isoformat(),
            Metadata.CUSTOM: {
                "columns": len(columns),
                "unmappedColumns": len(self.remapper.table.getUnmapped()),
                "rows": totalRows
            }
        })
        
        return True

    def applyAugments(self, df: pd.DataFrame) -> pd.DataFrame:
        for augment in self.augments:
            df = augment.run(args=[df])
        return df
    
    def package(self, compressLocation: Path) -> Path:
        renamedFile = self.metadataPath.rename(self.output.filePath / self.metadataPath.name)
        outputPath = zp.compress(self.output.filePath, compressLocation)
        renamedFile.rename(self.metadataPath)
        return outputPath
    
class ColumnFiller:
    def __init__(self, fillProperties: dict[str, dict]):
        self.fillProperties = fillProperties

        for event, columns in self.fillProperties.items():
            if not self._validEvent(event):
                raise Exception(f"Unknown event: {event}") from AttributeError
            
            for mapToDict in columns.values():
                for mapToEvent in mapToDict:
                    if not self._validEvent(mapToEvent):
                        raise Exception(f"Unknown mapTo event: {event}") from AttributeError

    def _validEvent(self, event: str) -> bool:
        return event in Event._value2member_map_
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        for event, columns in self.fillProperties.items():
            for columnName, mapTo in columns.items():
                for mapToEvent, mapToColumnList in mapTo.items():
                    for mapToColumn in mapToColumnList:
                        df[(mapToEvent, mapToColumn)].fillna(df[(event, columnName)], inplace=True)

        return df
