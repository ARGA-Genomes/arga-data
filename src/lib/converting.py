from lib.processing.files import DataFile
import pandas as pd
import logging
from lib.bigFiles import StackedDFWriter
import gc
from pathlib import Path
from pyoxigraph import Store, RdfFormat, NamedNode, Triple

class Converter:

    _mapFileName = "map.json"

    _datasetIDLabel = "dataset_id"
    _entityIDLabel = "entity_id"
    _entityIDEvent = "collection"

    def __init__(self, inputFile: DataFile, outputPath: Path, mapPath: Path):
        self.inputFile = inputFile
        self.outputPath = outputPath
        self.mapPath = mapPath
        
        self._map = {}

    def _loadMap(self) -> None:
        def _nodeName(node: NamedNode):
            return node.value.rsplit("/", 1)[-1]

        def _addData(graphName: str, name: str, data: dict) -> None:
            if name not in self.map[graphName]:
                self.map[graphName][name] = {}

            self.map[graphName][name] |= data

        store = Store()
        with open(self.mapPath, "rb") as fp:
            store.load(fp, RdfFormat.TRIG)

        for graph in store.named_graphs():
            graphName = _nodeName(graph)
            if graphName not in self.map:
                self.map[graphName] = {}

            for quad in store.quads_for_pattern(None, None, None, graph):
                if isinstance(quad.object, NamedNode): # Normal translation
                    nodeName = _nodeName(quad.subject)
                    _addData(graphName, nodeName, {"method": _nodeName(quad.predicate), "source": _nodeName(quad.object)})

                elif isinstance(quad.object, Triple): # Other requirement
                    for nestedQuad in store.quads_for_pattern(None, None, quad.subject, graph):
                        nodeName = _nodeName(nestedQuad.subject)
                        _addData(graphName, nodeName, {"condition_method": _nodeName(nestedQuad.predicate), "condition": [_nodeName(quad.object.subject), _nodeName(quad.object.predicate), _nodeName(quad.object.object)]})
    
    def _apply(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        ...

    def convert(self, chunkSize: int, verbose: bool) -> tuple[bool, dict]:
        self._loadMap()

        logging.info("Processing chunks for conversion")
        

        writer = StackedDFWriter(self.outputPath, list(self.map))

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

                processedSections = self._apply(df)
                if not processedSections:
                    return False, {}

                writer.write(processedSections, idx-1)

            del df
            gc.collect()

        writer.combine(removeParts=True)

        metadata = {
            "total columns": len(self.inputFile.getColumns()),
            "rows": totalRows
        }

        return True, metadata
