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

    def __init__(self, inputFile: DataFile, outputPath: Path):
        self.inputFile = inputFile
        self.outputPath = outputPath

    def convert(self, map: Map, chunkSize: int, datasetID: str, entityEvent: str, entityColumn: str, verbose: bool) -> tuple[bool, dict]:
        logging.info("Processing chunks for conversion")

        def _processChunk(chunk: pd.DataFrame) -> dict[str, pd.DataFrame]:
            dfEvents = map.applyTo(chunk) # Returns a multi-index dataframe
            
            if not dfEvents:
                return {}
            
            error = f"Unable to generate '{self._entityIDLabel}':"
            if entityEvent not in dfEvents:
                logging.error(f"{error} no event found '{entityEvent}'")
                return {}
            
            if entityColumn not in dfEvents[entityEvent].columns:
                logging.error(f"{error} dataset is missing field '{entityColumn}' in event '{entityEvent}'")
                return {}
            
            dfEvents[self._entityIDEvent][self._datasetIDLabel] = datasetID
            dfEvents[self._entityIDEvent][self._entityIDLabel] = dfEvents[self._entityIDEvent][self._datasetIDLabel] + dfEvents[entityEvent][entityColumn]
            return dfEvents
    
        writer = StackedDFWriter(self.outputPath, map.events)

        totalRows = 0
        chunks = self.inputFile.readIterator(chunkSize, low_memory=False)
        completed = writer.completedCount()

        if completed > 0:
            logging.info(f"Already completed {completed} chunks, resuming...")

        for idx, df in enumerate(chunks, start=1):
            totalRows += len(df)

            if idx > completed:
                if verbose:
                    print(f"At chunk: {idx}", end='\r')

                dfSections = _processChunk(df)
                if not dfSections:
                    return False, {}

                writer.write(dfSections, idx-1)

            del df
            gc.collect()

        writer.combine(removeParts=True)

        metadata = {
            "total columns": len(self.inputFile.getColumns()),
            "unmapped columns": writer.uniqueColumns(map._unmappedLabel),
            "rows": totalRows
        }

        return True, metadata
