from lib.processing.files import DataFile
import pandas as pd
import logging
from lib.bigFiles import DFWriter
import gc
from pathlib import Path
from pyoxigraph import Store, RdfFormat, NamedNode
import hashlib

class Map:
    def __init__(self):
        self._data = {}

    @staticmethod
    def _hash(value: any) -> str:
        return hashlib.md5(str(value).encode("utf-8")).hexdigest()

    @staticmethod
    def _getNodeName(node: NamedNode) -> str:
        return node.value.rsplit("/", 1)[-1]
    
    def load(self, path: Path) -> list[str]:
        store = Store()
        with open(path, "rb") as fp:
            store.load(fp, RdfFormat.TRIG)

        for graph in store.named_graphs():
            graphName = self._getNodeName(graph)
            if graphName not in self._data:
                self._data[graphName] = {}

            for quad in store.quads_for_pattern(None, None, None, graph):
                newColumn = self._getNodeName(quad.subject)
                if newColumn not in self._data[graphName]:
                    self._data[graphName][newColumn] = []

                self._data[graphName][newColumn].append((self._getNodeName(quad.predicate), self._getNodeName(quad.object)))

    def apply(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        mappedData = {}
        for graph, columnInfo in self._data.items():
            sectionData = {}
            for newColumn, columnnSources in columnInfo.items():
                for method, source in columnnSources:
                    sectionData[newColumn] = df[source] if method == "same" else df[source].apply(self._hash)

            mappedData[graph] = pd.DataFrame.from_dict(sectionData)

        return mappedData

    def getGraphs(self) -> list[str]:
        return list(self._data)

class Converter:
    def __init__(self, inputFile: DataFile, outputDir: Path, mapPath: Path):
        self.inputFile = inputFile
        self.outputDir = outputDir
        self.mapPath = mapPath

    def convert(self, chunkSize: int, verbose: bool) -> tuple[bool, dict]:
        map = Map()
        map.load(self.mapPath)
        
        writers: dict[str, DFWriter] = {}
        for graph in map.getGraphs():
            writers[graph] = DFWriter(self.outputDir / f"{graph}.csv", subDirName=graph)

        totalRows = 0
        chunks = self.inputFile.readIterator(chunkSize, low_memory=False)
        completed = min(writer.writtenFileCount() for writer in writers.values())

        if completed > 0:
            logging.info(f"Already completed {completed} chunks, resuming...")

        logging.info("Processing chunks for conversion")
        for idx, df in enumerate(chunks, start=1):
            totalRows += len(df)

            if idx > completed:
                if verbose:
                    print(f"At chunk: {idx}", end='\r')

                processedSections = map.apply(df)
                if not processedSections:
                    return False, {}

                for graph, df in processedSections.items():
                    writers[graph].write(df, index=(idx-1))

            del df
            gc.collect()

        for writer in writers.values():
            writer.combine(removeParts=True)

        metadata = {
            "total columns": len(self.inputFile.getColumns()),
            "rows": totalRows
        }

        return True, metadata
