import pandas as pd
from pathlib import Path
import lib.common as cmn
from lib.bigFileWriter import BigFileWriter
from lib.processing.mapping import Remapper, Event
from lib.processing.stages import File, StackedFile
from lib.processing.scripts import FunctionScript
from lib.systemManagers.baseManager import SystemManager, Metadata
import logging
import gc
import time
from datetime import datetime, date
import lib.zipping as zp

class ConversionManager(SystemManager):
    def __init__(self, baseDir: Path, dataDir: Path, datasetID: str, location: str, database: str, subsection: str):
        self.stepName = "conversion"

        super().__init__(baseDir, self.stepName, "tasks")

        self.conversionDir = dataDir / self.stepName

        self.datasetID = datasetID
        self.location = location
        self.database = database
        self.subsection = subsection

        self.file = None

    def _generateFileName(self, withTimestamp: bool) -> StackedFile:
        sourceName = f"{self.location}-{self.database}"
        if self.subsection:
            sourceName += f"-{self.subsection}"
        
        if withTimestamp:
            sourceName += date.today().strftime("-%Y-%m-%d")

        return StackedFile(self.conversionDir / sourceName)

    def loadFile(self, file: File, properties: dict, mapDir: Path) -> None:
        self.file = file

        self.mapID = properties.pop("mapID", -1)
        self.customMapID = properties.pop("customMapID", -1)
        self.customMapPath = properties.pop("customMapPath", None)
        if self.customMapPath is not None:
            self.customMapPath = Path(self.customMapPath)

        self.chunkSize = properties.pop("chunkSize", 1024)
        self.skipRemap = properties.pop("skipRemap", [])
        self.preserveDwC = properties.pop("preserveDwC", False)
        self.prefixUnmapped = properties.pop("prefixUnmapped", True)

        timestamp = properties.pop("timestamp", True)
        self.output = self._generateFileName(timestamp)

        self.augments = [FunctionScript(self.baseDir, augProperties) for augProperties in properties.pop("augment", [])]

        self.remapper = Remapper(mapDir, self.mapID, self.customMapID, self.customMapPath, self.location, self.preserveDwC, self.prefixUnmapped)

    def convert(self, overwrite: bool = False, verbose: bool = True, ignoreRemapErrors: bool = True, forceRetrieve: bool = False) -> bool:
        if self.file is None:
            logging.error("No file loaded for conversion, exiting...")
            return False

        if self.datasetID is None:
            logging.error("No datasetID provided which is required for conversion, exiting...")
            return False

        if self.output.filePath.exists() and not overwrite:
            logging.info(f"{self.output.filePath} already exists, exiting...")
            return True
        
        # Get columns and create mappings
        logging.info("Getting column mappings")
        columns = cmn.getColumns(self.file.filePath, self.file.separator, self.file.firstRow)

        success = self.remapper.buildTable(columns, self.skipRemap, forceRetrieve)
        if not success:
            return False
        
        if not self.remapper.table.allUniqueColumns(): # If there are non unique columns
            if not ignoreRemapErrors:
                for event, firstCol, matchingCols in self.remapper.table.getNonUnique():
                    for col in matchingCols:
                        logging.warning(f"Found mapping for column '{col}' that matches initial mapping '{firstCol}' under event '{event.value}'")
                return False
            
            self.remapper.table.forceUnique()
        
        logging.info("Resolving events")
        writers: dict[str, BigFileWriter] = {}
        for event in self.remapper.table.getEventCategories():
            cleanedName = event.value.lower().replace(" ", "_")
            writers[event] = BigFileWriter(self.output.filePath / f"{cleanedName}.csv", f"{cleanedName}_chunks")

        logging.info("Processing chunks for conversion")

        totalRows = 0
        startTime = time.perf_counter()

        chunks = cmn.chunkGenerator(self.file.filePath, self.chunkSize, self.file.separator, self.file.firstRow, self.file.encoding)
        for idx, df in enumerate(chunks, start=1):
            if verbose:
                print(f"At chunk: {idx}", end='\r')

            df = self.remapper.applyTranslation(df) # Returns a multi-index dataframe
            df = self.applyAugments(df)

            if df is None:
                return False

            datasetID = "dataset_id"
            scientificName = "scientific_name"
            entityID = "entity_id"

            if scientificName not in df[Event.COLLECTION].columns:
                logging.error(f"Unable to generate '{entityID}' as dataset is missing field '{scientificName}' in event '{Event.COLLECTION.value}'")
                return False
            
            df[(Event.COLLECTION, datasetID)] = self.datasetID
            df[(Event.COLLECTION, entityID)] = df[(Event.COLLECTION, datasetID)] + df[(Event.COLLECTION, scientificName)]

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

    def applyAugments(self, df: pd.DataFrame) -> pd.DataFrame | None:
        for augment in self.augments:
            success, df = augment.run(False, inputArgs=[df])

            if not success:
                return None

            if not isinstance(df, pd.DataFrame):
                logging.error(f"Augment '{augment.function}' does not output dataframe as required, instead output {type(df)}")
                return None
            
        return df
    
    def package(self, compressLocation: Path) -> Path | None:
        outputFileName = self.getMetadata(-1, Metadata.OUTPUT)
        if outputFileName is None:
            return
        
        outputFilePath = self.conversionDir / outputFileName
        renamedFile = self.metadataPath.rename(outputFilePath / self.metadataPath.name)
        outputPath = zp.compress(outputFilePath, compressLocation)
        renamedFile.rename(self.metadataPath)
        
        return outputPath
