from pathlib import Path
import pandas as pd
import logging
import lib.processing.files as files
from lib.processing.files import DataFormat, DataFile, Folder, StackedFile
from typing import Iterator
from lib.json import JsonSynchronizer

class DFWriter:

    _chunkPrefix = "chunk"
    _metaFilePaths = "filePaths"

    def __init__(self, outputFilePath: Path, chunkFormat: DataFormat = DataFormat.PARQUET, subDirName: str = "bigFileWriter", loadOnInit: bool = True):
        self.outputFile = DataFile(outputFilePath)
        self._chunkFormat = chunkFormat

        self.workingDir = Folder(outputFilePath.parent / subDirName, create=True)
        self.metadata = JsonSynchronizer(self.workingDir.path / "metadata.json")

        self._sectionFiles: list[DataFile] = []
        self._uniqueColumns: dict[str, None] = {}

        if loadOnInit:
            self._loadFiles()

            if self._sectionFiles:
                logging.info(f"Added {len(self._sectionFiles)} existing files from working directory '{self.workingDir.path}'")

    def _loadFiles(self) -> None:
        for filePath in self.metadata.get(self._metaFilePaths, []):
            dataFile = DataFile(filePath)
            self._sectionFiles.append(dataFile)
            self._uniqueColumns |= {column: None for column in dataFile.getColumns()}

    def _wroteFile(self, name: str) -> None:
        if self.metadata.get(self._metaFilePaths) is None:
            self.metadata[self._metaFilePaths] = [name]
        else:
            self.metadata[self._metaFilePaths] += [name]

    def writtenFileCount(self) -> int:
        return len(self._sectionFiles)
    
    def uniqueColumns(self) -> list[str]:
        return list(self._uniqueColumns.keys())

    def write(self, df: pd.DataFrame, fileName: str = "") -> None:
        if not fileName:
            fileName = f"{self._chunkPrefix}_{len(self._sectionFiles)}"
            
        subfile = DataFile(self.workingDir.path / (fileName + self._chunkFormat.value))
        subfile.write(df, index=False)
        self._wroteFile(fileName)

        self._sectionFiles.append(subfile)
        self._uniqueColumns |= {column: None for column in df.columns}

    def combine(self, readChunkSize: int = 1024, removeParts: bool = False, **kwargs) -> None:
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
            self.outputFile.writeIterator(combinedIterator(self._sectionFiles, readChunkSize), list(self._uniqueColumns), **kwargs)
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
        super().write(pd.DataFrame.from_records(self._records))
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

    def combine(self, readChunkSize: int = 1024, removeParts: bool = False, **kwargs) -> None:
        if self._records:
            self._writeRecords()

        super().combine(readChunkSize, removeParts, **kwargs)

def combinedIterator(dataFiles: list[DataFile], chunkSize: int, **kwargs: dict) -> Iterator[pd.DataFrame]:
    for file in dataFiles:
        for chunk in file.readIterator(chunkSize, **kwargs):
            yield chunk

def combineDirectoryFiles(outputFilePath: Path, inputFolderPath: Path, matchPattern: str = "*.*", chunkSize: int = 1024, deleteOld: bool = False, **kwargs: dict) -> None:
    inputDataFiles = [dataFile for dataFile in  [DataFile(path) for path in inputFolderPath.glob(matchPattern)] if dataFile.format != DataFormat.UNKNOWN and dataFile.format != DataFormat.STACKED]
    logging.info(f"Found {len(inputDataFiles)} files to combine")
    columns = [column for dataFile in inputDataFiles for column in dataFile.getColumns()]
    combineDataFiles(outputFilePath, inputDataFiles, columns, chunkSize, deleteOld, **kwargs)

def combineDataFiles(outputFilePath: Path, dataFiles: list[DataFile], columns: list[str], chunkSize: int = 1024, deleteOld: bool = False, **kwargs: dict) -> None:
    outputDataFile = DataFile(outputFilePath)
    logging.info(f"Combining into one file at {outputFilePath}")
    outputDataFile.writeIterator(combinedIterator(dataFiles, chunkSize), columns, index=False, **kwargs)
    logging.info(f"Successfully combined into a single file")

    if deleteOld:
        logging.info(f"Cleaning up old sections of combined file")
        for dataFile in dataFiles:
            dataFile.delete()

class StackedDFWriter:
    def __init__(self, outputFilePath: Path, subsections: list[str], chunkFormat: DataFormat = DataFormat.PARQUET):
        self.outputFile = StackedFile(outputFilePath)
        self._subWriters = {subsection: DFWriter(outputFilePath / f"{subsection}.csv", chunkFormat=chunkFormat, subDirName=subsection) for subsection in subsections}
    
    def uniqueColumns(self, subsection: str) -> list[str]:
        return self._subWriters[subsection].uniqueColumns()

    def write(self, df: pd.DataFrame) -> None: # Expects multilayer dataframe
        for outerColumn in df.columns.levels[0]:
            self._subWriters[outerColumn].write(df[outerColumn])

    def combine(self, removeParts: bool = False) -> None:
        for writer in self._subWriters.values():
            writer.combine(removeParts=removeParts)
