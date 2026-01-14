from lib.processing.mapping import Map
from lib.processing.files import DataFile, StackedFile
import pandas as pd
import logging
from lib.bigFiles import StackedDFWriter
import gc
from pathlib import Path

class Converter:

    _mapFileName = "map.json"

    _datasetIDLabel = "dataset_id"
    _entityIDLabel = "entity_id"
    _entityIDEvent = "collection"

    def __init__(self, mapDir: Path, inputFile: DataFile, outputFile: StackedFile, prefix: str, datasetID: str, entityInfo: tuple[str, str], chunkSize: int):
        self.mapDir = mapDir
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.prefix = prefix
        self.datasetID = datasetID
        self.entityEvent, self.entityColumn = entityInfo
        self.chunkSize = chunkSize

        self.map = None

    def loadMap(self, mapID: str, mapColumnName: str, forceRetrieve: bool) -> None:
        mapFile = self.mapDir / self._mapFileName

        if mapFile.exists() and not forceRetrieve:
            logging.info("Using local map file")
            self.map = Map.fromFile(mapFile)
        elif mapColumnName:
            logging.info("Using updated mapping sheet")
            self.map = Map.fromModernSheet(mapColumnName, mapFile)
        elif mapID > 0:
            logging.info("Using original mapping sheet")
            self.map = Map.fromSheets(mapID, mapFile)
        else:
            logging.warning("No mapping found")

        if self.map is None or self.map.isEmpty():
            raise Exception("Unable to load map file") from FileNotFoundError

    def _processChunk(self, chunk: pd.DataFrame) -> pd.DataFrame | None:
        df = self.map.applyTo(chunk, self.prefix) # Returns a multi-index dataframe
        
        if df is None:
            return
        
        error = f"Unable to generate '{self._entityIDLabel}':"
        if self.entityEvent not in df.columns:
            logging.error(f"{error} no event found '{self.entityEvent}'")
            return
        
        if self.entityColumn not in df[self.entityEvent].columns:
            logging.error(f"{error} dataset is missing field '{self.entityColumn}' in event '{self.entityEvent}'")
            return
        
        df[(self._entityIDEvent, self._datasetIDLabel)] = self.datasetID
        df[(self._entityIDEvent, self._entityIDLabel)] = df[(self._entityIDEvent, self._datasetIDLabel)] + df[(self.entityEvent, self.entityColumn)]
        return df

    def convert(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        logging.info("Processing chunks for conversion")
        writer = StackedDFWriter(self.outputFile.path, self.map.events)

        totalRows = 0
        chunks = self.inputFile.readIterator(self.chunkSize, low_memory=False)
        for idx, df in enumerate(chunks, start=1):
            if verbose:
                print(f"At chunk: {idx}", end='\r')

            df = self._processChunk(df)
            if df is None:
                return False, {}

            writer.write(df)

            totalRows += len(df)
            del df
            gc.collect()

        writer.combine(removeParts=True)

        metadata = {
            "total columns": len(self.inputFile.getColumns()),
            "unmapped columns": writer.uniqueColumns(self.map._unmappedLabel),
            "rows": totalRows
        }

        return True, metadata
