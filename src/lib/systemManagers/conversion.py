import pandas as pd
from pathlib import Path
from lib.bigFileWriter import BigFileWriter
from lib.processing.mapping import Map
from lib.processing.stages import File, StackedFile
from lib.processing.scripts import FunctionScript
from lib.systemManagers.baseManager import SystemManager, Metadata
import logging
import gc
import time
from datetime import datetime, date
import lib.zipping as zp

class ConversionManager(SystemManager):
    def __init__(self, baseDir: Path, dataDir: Path, mapDir: Path, datasetID: str, prefix: str, name: str):
        self.stepName = "conversion"

        super().__init__(baseDir, self.stepName, "tasks")

        self.conversionDir = dataDir / self.stepName

        self.datasetID = datasetID
        self.prefix = prefix
        self.name = name

        self.inputFile = None
        self.mapFile = mapDir / "map.json"
        self.map = {}

    def _generateFileName(self, withTimestamp: bool) -> StackedFile:
        outputName = f"{self.name}{date.today().strftime('-%Y-%m-%d') if withTimestamp else ''}"
        return StackedFile(self.conversionDir / outputName)
    
    def _getMap(self, mapID: int, mapColumnName: str, forceRetrieve: bool) -> Map:
        if self.mapFile.exists() and not forceRetrieve:
            return Map.fromFile(self.mapFile)
        
        if mapColumnName:
            return Map.fromModernSheet(mapColumnName, self.mapFile)

        if mapID > 0:
            return Map.fromSheets(mapID, self.mapFile)
        
        logging.warning(f"No mapping found for dataset {self.name}")
        return Map()

    def prepare(self, file: File, properties: dict, forceRetrieve: bool) -> None:
        self.inputFile = file

        mapID = properties.pop("mapID", -1)
        mapColumnName = properties.pop("mapColumnName", "")
        self.map = self._getMap(mapID, mapColumnName, forceRetrieve)

        timestamp = properties.pop("timestamp", True)
        self.output = self._generateFileName(timestamp)

        self.chunkSize = properties.pop("chunkSize", 1024)
        self.augments = [FunctionScript(self.baseDir, augProperties) for augProperties in properties.pop("augment", [])]

    def convert(self, overwrite: bool = False, verbose: bool = True) -> bool:
        if self.inputFile is None:
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
        columns = self.inputFile.getColumns()
        
        logging.info("Resolving events")
        writers: dict[str, BigFileWriter] = {}
        for event in self.map.events:
            cleanedName = event.replace(" ", "_")
            writers[event] = BigFileWriter(self.output.filePath / f"{cleanedName}.csv", f"{cleanedName}_chunks")

        logging.info("Processing chunks for conversion")

        totalRows = 0
        startTime = time.perf_counter()

        chunks = self.inputFile.loadDataFrameIterator(self.chunkSize)
        for idx, df in enumerate(chunks, start=1):
            if verbose:
                print(f"At chunk: {idx}", end='\r')

            df = self.map.applyTo(df, self.prefix) # Returns a multi-index dataframe
            df = self.applyAugments(df)

            if df is None:
                return False

            datasetID = "dataset_id"
            scientificName = "scientific_name"
            canonicalName = "canonical_name"
            entityID = "entity_id"

            collection = "collection"

            if scientificName not in df[collection].columns:
                if canonicalName in df[collection].columns:
                    scientificName = canonicalName

                else:
                    logging.error(f"Unable to generate '{entityID}' as dataset is missing field '{scientificName}' in event '{collection}'")
                    return False
            
            df[(collection, datasetID)] = self.datasetID
            df[(collection, entityID)] = df[(collection, datasetID)] + df[(collection, scientificName)]

            for eventColumn in df.columns.levels[0]:
                writers[eventColumn].writeDF(df[eventColumn])

            totalRows += len(df)
            del df
            gc.collect()

        totalUnmapped = 0
        for writer in writers.values():
            totalUnmapped += sum(1 for column in writer.globalColumns if column not in self.map.translation)
            writer.oneFile()

        self.updateMetadata(0, {
            Metadata.OUTPUT: self.output.filePath.name,
            Metadata.SUCCESS: True,
            Metadata.DURATION: time.perf_counter() - startTime,
            Metadata.TIMESTAMP: datetime.now().isoformat(),
            Metadata.CUSTOM: {
                "columns": len(columns),
                "unmappedColumns": totalUnmapped,
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
