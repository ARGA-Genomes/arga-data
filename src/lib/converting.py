from lib.processing.files import DataFile
import pandas as pd
import logging
from lib.bigFiles import StackedDFWriter
import gc
from pathlib import Path
from pyoxigraph import Store, RdfFormat, NamedNode
import hashlib

class Converter:
    def __init__(self, inputFile: DataFile, outputPath: Path, mapPath: Path):
        self.inputFile = inputFile
        self.outputPath = outputPath
        self.mapPath = mapPath

        self._map: dict[str, dict[str, list[tuple[str, str]]]] = {}

    @staticmethod
    def _hash(value: any) -> str:
        return hashlib.md5(str(value).encode("utf-8")).hexdigest()

    def _loadMap(self) -> None:
        def _nodeName(node: NamedNode):
            return node.value.rsplit("/", 1)[-1]
        
        store = Store()
        with open(self.mapPath, "rb") as fp:
            store.load(fp, RdfFormat.TRIG)

        for graph in store.named_graphs():
            graphName = _nodeName(graph)
            if graphName not in self._map:
                self._map[graphName] = {}

            for quad in store.quads_for_pattern(None, None, None, graph):
                newColumn = _nodeName(quad.subject)
                if newColumn not in self._map[graphName]:
                    self._map[graphName][newColumn] = []

                self._map[graphName][newColumn].append((_nodeName(quad.predicate), _nodeName(quad.object)))

    def _apply(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        mappedData = {}
        for section, columnInfo in self._map.items():

            sectionData = {}
            for newColumn, columnnSources in columnInfo.items():
                for method, source in columnnSources:
                    sectionData[newColumn] = df[source] if method == "same" else df[source].apply(self._hash)

            mappedData[section] = pd.DataFrame.from_dict(sectionData)
        return mappedData

    def convert(self, chunkSize: int, verbose: bool) -> tuple[bool, dict]:
        self._loadMap()
        writer = StackedDFWriter(self.outputPath, list(self._map))

        totalRows = 0
        chunks = self.inputFile.readIterator(chunkSize, low_memory=False)
        completed = writer.completedCount()

        if completed > 0:
            logging.info(f"Already completed {completed} chunks, resuming...")

        logging.info("Processing chunks for conversion")
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
