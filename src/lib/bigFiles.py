import csv
from pathlib import Path
import lib.common as cmn
import pandas as pd
import sys
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from lib.progressBar import ProgressBar
import lib.processing.files as files
from lib.processing.files import DataFormat, DataFile, Folder

class DFWriter:

    _subDirName = "bigFileWriter"
    _chunkPrefix = "chunk"

    def __init__(self, outputFilePath: Path, chunkFormat: DataFormat = DataFormat.PARQUET):
        self.outputFile = DataFile(outputFilePath)
        self._chunkFormat = chunkFormat

        self.workingDir = Folder(outputFilePath.parent / self._subDirName, create=True)
        self._sectionFiles: list[DataFile] = []
        self._uniqueColumns: dict[str, None] = {}

        for path in self.workingDir.getMatchingPaths(f"{self._chunkPrefix}_*"):
            dataFile = DataFile(path)
            self._sectionFiles.append(dataFile)
            self._uniqueColumns |= {column: None for column in dataFile.getColumns()}

        if self._sectionFiles:
            logging.info(f"Added {len(self._sectionFiles)} existing files from working directory")

    def writtenFileCount(self) -> int:
        return len(self._sectionFiles)
    
    def uniqueColumns(self) -> list[str]:
        return list(self._uniqueColumns.keys())

    def write(self, df: pd.DataFrame) -> None:
        fileName = f"{self._chunkPrefix}_{len(self._sectionFiles)}{self._chunkFormat.value}"
        subfile = DataFile(self.workingDir.path / fileName)
        subfile.write(df, index=False)
        self._sectionFiles.append(subfile)
        self._uniqueColumns |= {column: None for column in df.columns}

    def combine(self, removeParts: bool = False) -> None:
        if self.outputFile.exists():
            logging.info(f"Removing old file {self.outputFile.path}")
            self.outputFile.delete()

        if len(self._sectionFiles) == 0:
            logging.warning(f"No files written, unable to create output file")
            return

        if len(self._sectionFiles) == 1:
            logging.info(f"Only single subfile, moving {self._sectionFiles[0].path} to {self.outputFile.path}")
            files.moveDataFile(self._sectionFiles[0], self.outputFile)
            return

        logging.info("Combining into one file")
        self.outputFile.writeIterator(files.combinedIterator(self._sectionFiles), self._uniqueColumns)
        logging.info(f"\nCreated a single file at {self.outputFile.path}")
        
        if removeParts:
            for file in self._sectionFiles:
                file.delete()

            self._sectionFiles.clear()
            self.workingDir.delete()

class RecordWriter(DFWriter):
    def __init__(self, outputFilePath: Path, rowsPerSubsection: int):
        super().__init__(outputFilePath)

        self._rowsPerSubsection = rowsPerSubsection
        self._records = []

    def _writeRecords(self) -> None:
        super().write(pd.DataFrame.from_records(self._records))
        self._records.clear()

    def write(self, record: dict) -> None:
        self._records.append(record)
        if len(self._records) == self._rowsPerSubsection:
            self._writeRecords()

    def combine(self, removeParts: bool = False) -> None:
        if self._records:
            self._writeRecords()

        super().combine(removeParts)
