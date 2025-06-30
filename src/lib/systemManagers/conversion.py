import pandas as pd
from pathlib import Path
from lib.bigFileWriter import BigFileWriter
from lib.processing.mapping import Map
from lib.processing.files import File, StackedFile, Step
from lib.processing.scripts import FunctionScript
from lib.systemManagers.baseManager import SystemManager, Task
import logging
import gc
from datetime import date
import lib.zipping as zp

class Conversion(Task):

    datasetIDLabel = "dataset_id"
    entityIDLabel = "entity_id"
    entityIDEvent = "collection"

    def __init__(self, prefix: str, datasetID: str, map: Map, inputFile: File, chunkSize: int, entityEvent: str, entityColumn: str, output: StackedFile, augments: list[FunctionScript]):
        self.prefix = prefix
        self.datasetID = datasetID
        self.map = map
        self.inputFile = inputFile
        self.entityEvent = entityEvent
        self.entityColumn = entityColumn
        self.chunkSize = chunkSize
        self.output = output
        self.augments = augments

        super().__init__()

    def getOutputPath(self) -> Path:
        return self.output.filePath

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        if self.output.filePath.exists() and not overwrite:
            logging.info(f"{self.getOutputPath()} already exists, exiting...")
            return True
        
        writers: dict[str, BigFileWriter] = {}
        for event in self.map.events:
            cleanedName = event.replace(" ", "_")
            writers[event] = BigFileWriter(self.output.filePath / f"{cleanedName}.csv", f"{cleanedName}_chunks")

        logging.info("Processing chunks for conversion")

        totalRows = 0
        chunks = self.inputFile.loadDataFrameIterator(self.chunkSize)
        for idx, df in enumerate(chunks, start=1):
            if verbose:
                print(f"At chunk: {idx}", end='\r')

            df = self._processChunk(df)
            if df is None:
                return False

            for eventColumn in df.columns.levels[0]:
                writers[eventColumn].writeDF(df[eventColumn])

            totalRows += len(df)
            del df
            gc.collect()

        totalUnmapped = 0
        for writer in writers.values():
            totalUnmapped += sum(1 for column in writer.globalColumns if column not in self.map.translation)
            writer.oneFile()

        self.setAdditionalMetadata(
            {
                "total columns": len(self.inputFile.getColumns()),
                "unmapped columns": totalUnmapped,
                "rows": totalRows
            }
        )

        return True
    
    def _processChunk(self, chunk: pd.DataFrame) -> pd.DataFrame | None:
        df = self.map.applyTo(chunk, self.prefix) # Returns a multi-index dataframe
        df = self._applyAugments(df)
        
        if df is None:
            return
        
        error = f"Unable to generate '{self.entityIDLabel}':"
        if self.entityEvent not in df.columns:
            logging.error(f"{error} no event found '{self.entityEvent}'")
            return
        
        if self.entityColumn not in df[self.entityEvent].columns:
            logging.error(f"{error} dataset is missing field '{self.entityColumn}' in event '{self.entityEvent}'")
            return
        
        df[(self.entityIDEvent, self.datasetIDLabel)] = self.datasetID
        df[(self.entityIDEvent, self.entityIDLabel)] = df[(self.entityIDEvent, self.datasetIDLabel)] + df[(self.entityEvent, self.entityColumn)]
        return df

    def _applyAugments(self, df: pd.DataFrame) -> pd.DataFrame | None:
        for augment in self.augments:
            success, df = augment.run(False, inputArgs=[df])

            if not success:
                return None

            if not isinstance(df, pd.DataFrame):
                logging.error(f"Augment '{augment.function}' does not output dataframe as required, instead output {type(df)}")
                return None
            
        return df

class ConversionManager(SystemManager):
    def __init__(self, dataDir: Path, scriptDir: Path, metadataDir: Path, datasetID: str, prefix: str, name: str):
        super().__init__(dataDir, scriptDir, metadataDir, Step.CONVERSION, "tasks")

        self.datasetID = datasetID
        self.prefix = prefix
        self.name = name

        self.mapFile = self.metadataDir / "map.json"

    def _generateFileName(self, withTimestamp: bool) -> StackedFile:
        outputName = f"{self.name}{date.today().strftime('-%Y-%m-%d') if withTimestamp else ''}"
        return StackedFile(self.workingDir / outputName)
    
    def _getMap(self, mapID: int, mapColumnName: str, forceRetrieve: bool) -> Map | None:
        if self.mapFile.exists() and not forceRetrieve:
            return Map.fromFile(self.mapFile)
        
        if mapColumnName:
            return Map.fromModernSheet(mapColumnName, self.mapFile)

        if mapID > 0:
            return Map.fromSheets(mapID, self.mapFile)
        
        logging.warning(f"No mapping found for dataset {self.name}")
        return None

    def prepare(self, file: File, properties: dict, forceRetrieve: bool) -> None:
        mapID = properties.pop("mapID", -1)
        mapColumnName = properties.pop("mapColumnName", "")
        map = self._getMap(mapID, mapColumnName, forceRetrieve)

        if map is None:
            logging.error(f"Unable to retrieve a map for {self.name}")
            return
        
        entityEvent = properties.pop("entityEvent", "collection")
        entityColumn = properties.pop("entityColumn", "scientific_name")

        timestamp = properties.pop("timestamp", True)
        output = self._generateFileName(timestamp)

        chunkSize = properties.pop("chunkSize", 1024)
        augments = [FunctionScript(self.scriptDir, augProperties) for augProperties in properties.pop("augment", [])]

        self._tasks.append(Conversion(self.prefix, self.datasetID, map, file, chunkSize, entityEvent, entityColumn, output, augments))

    def convert(self, overwrite: bool = False, verbose: bool = True) -> bool:
        if not self._tasks:
            logging.error("No file loaded for conversion, exiting...")
            return False

        if self.datasetID is None:
            logging.error("No datasetID provided which is required for conversion, exiting...")
            return False
        
        return self.runTasks(overwrite, verbose)
    
    def package(self, compressLocation: Path) -> Path | None:
        outputFileName = self.getLastOutput()
        if outputFileName is None:
            return
        
        outputFilePath = self.dataDir / outputFileName
        renamedFile = self.metadataPath.rename(outputFilePath / self.metadataPath.name)
        outputPath = zp.compress(outputFilePath, compressLocation)
        renamedFile.rename(self.metadataPath)
        
        return outputPath
