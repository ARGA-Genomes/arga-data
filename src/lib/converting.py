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

    def _processChunk(self, chunk: pd.DataFrame) -> dict[str, pd.DataFrame]:
        dfEvents = self.map.applyTo(chunk, self.prefix) # Returns a multi-index dataframe
        
        if not dfEvents:
            return {}
        
        error = f"Unable to generate '{self._entityIDLabel}':"
        if self.entityEvent not in dfEvents:
            logging.error(f"{error} no event found '{self.entityEvent}'")
            return {}
        
        if self.entityColumn not in dfEvents[self.entityEvent].columns:
            logging.error(f"{error} dataset is missing field '{self.entityColumn}' in event '{self.entityEvent}'")
            return {}
        
        dfEvents[self._entityIDEvent][self._datasetIDLabel] = self.datasetID
        dfEvents[self._entityIDEvent][self._entityIDLabel] = dfEvents[self._entityIDEvent][self._datasetIDLabel] + dfEvents[self.entityEvent][self.entityColumn]
        return dfEvents

    def convert(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        logging.info("Processing chunks for conversion")
        writer = StackedDFWriter(self.outputFile, self.map.events)

        totalRows = 0
        chunks = self.inputFile.readIterator(self.chunkSize, low_memory=False)
        completed = writer.completedCount()

        if completed > 0:
            logging.info(f"Already completed {completed} chunks, resuming...")

        for idx, df in enumerate(chunks, start=1):
            totalRows += len(df)

            if idx > completed:
                if verbose:
                    print(f"At chunk: {idx}", end='\r')

                dfSections = self._processChunk(df)
                if not dfSections:
                    return False, {}

                writer.write(dfSections, idx-1)

            del df
            gc.collect()

        writer.combine(removeParts=True)

        metadata = {
            "total columns": len(self.inputFile.getColumns()),
            "unmapped columns": writer.uniqueColumns(self.map._unmappedLabel),
            "rows": totalRows
        }

        return True, metadata
