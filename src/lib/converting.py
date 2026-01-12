from lib.processing.mapping import Map
from lib.processing.files import DataFile, StackedFile
import pandas as pd
import logging
from lib.bigFiles import StackedDFWriter
import gc
from pathlib import Path

class Converter:

    _mapFileName = "map.json"

    def __init__(self, mapDir: Path, inputFile: DataFile, outputFile: StackedFile, prefix: str, entityInfo: tuple[str, str], chunkSize: int):
        self.mapDir = mapDir
        self.inputFile = inputFile
        self.outputFile = outputFile
        self.prefix = prefix
        self.entityInfo = entityInfo
        self.chunkSize = chunkSize

        self.map = None

    def loadMap(self, mapID: str, mapColumnName: str, forceRetrieve: bool) -> None:
        mapFile = self.mapDir / self._mapFileName

        if mapFile.exists() and not forceRetrieve:
            self.map = Map.fromFile(mapFile)

        elif mapColumnName:
            self.map = Map.fromModernSheet(mapColumnName, mapFile)

        elif mapID > 0:
            self.map = Map.fromSheets(mapID, mapFile)
        
        logging.warning("No mapping found")
    
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

    def convert(self, overwrite: bool, verbose: bool) -> tuple[bool, dict]:
        logging.info("Processing chunks for conversion")
        writer = StackedDFWriter(self.outputFile.path, self.map.events)

        totalRows = 0
        chunks = self.inputFile.readIterator(self.chunkSize)
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
