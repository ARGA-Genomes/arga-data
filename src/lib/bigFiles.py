from pathlib import Path
import polars as pl
import logging
import lib.processing.files as files
from lib.processing.files import DataFormat, DataFile, Folder, StackedFile
from typing import Iterator
from lib.json import JsonSynchronizer

class DFWriter:

    _chunkPrefix = "chunk"
    _metaFilenames = "fileNames"

    def __init__(self, outputFilePath: Path, chunkFormat: DataFormat = DataFormat.PARQUET, subDirName: str = "bigFileWriter", loadOnInit: bool = True):
        self.outputFile = DataFile(outputFilePath)
        self._chunkFormat = chunkFormat

        self.workingDir = Folder(outputFilePath.parent / subDirName, create=True)
        self.metadata = JsonSynchronizer(self.workingDir.path / "metadata.json")

        self._sectionFiles: list[DataFile] = []
        self._schema = pl.Schema()

        if loadOnInit:
            self._loadFiles()

            if self._sectionFiles:
                logging.info(f"Added {len(self._sectionFiles)} existing files from working directory '{self.workingDir.path}'")

    def _loadFiles(self) -> None:
        for fileName in self.metadata.get(self._metaFilenames, []):
            dataFile = DataFile(self.workingDir.path / fileName)
            self._sectionFiles.append(dataFile)
            self._schema.update(dataFile.getSchema())

    def _wroteFile(self, name: str) -> None:
        if self.metadata.get(self._metaFilenames) is None:
            self.metadata[self._metaFilenames] = [name]
        else:
            self.metadata[self._metaFilenames].append(name)

    def writtenFileCount(self) -> int:
        return len(self._sectionFiles)
    
    def uniqueColumns(self) -> list[str]:
        return list(self._schema.names())

    def write(self, df: pl.DataFrame, fileName: str = "", index: int = -1) -> None:
        if not fileName:
            fileName = f"{self._chunkPrefix}_{len(self._sectionFiles) if index < 0 else index}"
            
        subfile = DataFile(self.workingDir.path / (fileName + self._chunkFormat.value))
        subfile.write(df)
        self._wroteFile(subfile.path.name)

        self._sectionFiles.append(subfile)
        self._schema.update(subfile.getSchema())

    def combine(self, removeParts: bool = False, **kwargs) -> None:
        if self.outputFile.exists():
            logging.info(f"Removing old file {self.outputFile.path}")
            self.outputFile.delete()

        if len(self._sectionFiles) == 0:
            logging.warning(f"No files written, unable to create output file")
            return

        if len(self._sectionFiles) == 1:
            logging.info(f"Only single subfile, moving {self._sectionFiles[0].path} to {self.outputFile.path}")
            files.moveDataFile(self._sectionFiles[0], self.outputFile)
        else:
            logging.info("Combining into one file")
            self.outputFile.sink(lazyCombine(self._sectionFiles, self._schema), **kwargs)
            logging.info(f"Created a single file at {self.outputFile.path}")
        
        if removeParts:
            self._sectionFiles.clear()
            self.workingDir.delete()

class RecordWriter(DFWriter):
    
    _metaRows = "rowsPerSubsection"

    def __init__(self, outputFilePath: Path, rowsPerSubsection: int, chunkFormat: DataFormat = DataFormat.PARQUET, subDirName: str = "bigFileWriter"):
        super().__init__(outputFilePath, chunkFormat, subDirName, False)

        self._rowsPerSubsection = rowsPerSubsection
        self._records = []

        if self.metadata.get(self._metaRows, -1) != rowsPerSubsection: # Different chunk size used from previous, throw out results
            self.metadata.clear()
            self.metadata[self._metaRows] = rowsPerSubsection
        else:
            self._loadFiles()

    def _writeRecords(self) -> None:
        super().write(pl.from_records(self._records))
        self._records.clear()

    def writtenRecordCount(self) -> int:
        return self.writtenFileCount() * self._rowsPerSubsection

    def write(self, record: dict) -> None:
        self._records.append(record)
        if len(self._records) == self._rowsPerSubsection:
            self._writeRecords()

    def writerMultipleRecords(self, records: list[dict]) -> None:
        for record in records:
            self.write(record)

    def combine(self, removeParts: bool = False, **kwargs) -> None:
        if self._records:
            self._writeRecords()

        super().combine(removeParts, **kwargs)

def lazyCombine(dataFiles: list[DataFile], schema: pl.Schema, **kwargs: dict) -> pl.LazyFrame:
    return pl.concat([file.scan(**kwargs).sort(schema.names()) for file in dataFiles])

def combineDirectoryFiles(outputFilePath: Path, inputFolderPath: Path, matchPattern: str = "*.*", deleteOld: bool = False, **kwargs: dict) -> None:
    inputDataFiles = [dataFile for dataFile in  [DataFile(path) for path in inputFolderPath.glob(matchPattern)] if dataFile.format != DataFormat.UNKNOWN and dataFile.format != DataFormat.STACKED]
    logging.info(f"Found {len(inputDataFiles)} files to combine")
    combineDataFiles(outputFilePath, inputDataFiles, deleteOld, **kwargs)

def combineDataFiles(outputFilePath: Path, dataFiles: list[DataFile], deleteOld: bool = False, **kwargs: dict) -> None:
    outputDataFile = DataFile(outputFilePath)
    logging.info(f"Combining into one file at {outputFilePath}")
    
    schema = pl.Schema()
    for file in dataFiles:
        schema.update(file.getSchema())

    outputDataFile.sink(lazyCombine(dataFiles, schema), **kwargs)
    logging.info(f"Successfully combined into a single file")

    if deleteOld:
        logging.info(f"Cleaning up old sections of combined file")
        for dataFile in dataFiles:
            dataFile.delete()

class StackedDFWriter:
    def __init__(self, outputFile: StackedFile, subsections: list[str], chunkFormat: DataFormat = DataFormat.PARQUET):
        self.outputFile = outputFile
        self._subWriters = {subsection: DFWriter(outputFile.path / f"{subsection}.csv", chunkFormat=chunkFormat, subDirName=subsection) for subsection in subsections}

    def uniqueColumns(self, subsection: str) -> list[str]:
        return self._subWriters[subsection].uniqueColumns()

    def write(self, dfSections: dict[str, pl.DataFrame], index: int = -1) -> None:
        for sectionName, df in dfSections.items():
            self._subWriters[sectionName].write(df, index=index)

    def combine(self, removeParts: bool = False) -> None:
        for writer in self._subWriters.values():
            writer.combine(removeParts=removeParts)

    def completedCount(self) -> int:
        return min(writer.writtenFileCount() for writer in self._subWriters.values())
