import csv
from pathlib import Path
import lib.commonFuncs as cmn
import pandas as pd
import sys
from enum import Enum
import pyarrow as pa
import pyarrow.parquet as pq
from lib.tools.logger import Logger
from typing import Iterator

class Format(Enum):
    CSV = ".csv"
    TSV = ".tsv"
    PARQUET = ".parquet"

class Subfile:

    fileFormat = Format.CSV

    def __new__(cls, *args):
        subclassMap = {subclass.fileFormat: subclass for subclass in cls.__subclasses__()}
        subclass = subclassMap.get(args[-1], cls)
        return super().__new__(subclass)

    def __init__(self, location: Path, fileName: str, format: Format) -> 'Subfile':
        self.filePath = location / f"{fileName}{Format(format).value}"
        self.size = self.filePath.stat().st_size if self.filePath.exists() else 0

    def __repr__(self) -> str:
        return f"{self.filePath}"
    
    @classmethod
    def fromFilePath(cls, filePath: Path) -> 'Subfile':
        location = filePath.parent
        fileName = filePath.stem
        fileFormat = Format(filePath.suffix.lower())
        return cls(location, fileName, fileFormat)
    
    def write(self, df: pd.DataFrame) -> None:
        df.to_csv(self.filePath, index=False)
    
    def read(self, **kwargs) -> pd.DataFrame:
        return pd.read_csv(self.filePath, **kwargs)
    
    def readChunks(self, chunkSize: int, **kwargs) -> Iterator[pd.DataFrame]:
        return self.read(chunkSize=chunkSize, **kwargs)

    def rename(self, newFilePath: Path, newFileFormat: Format) -> None:
        if newFileFormat == self.fileFormat:
            self.filePath.rename(newFilePath)
            return
        
        df = self.read()
        
        if newFileFormat == Format.CSV:
            df.to_csv(newFilePath, index=False)

        elif newFileFormat == Format.TSV:
            df.to_csv(newFilePath, sep="\t", index=False)
            
        elif newFileFormat == Format.PARQUET:
            df.to_parquet(newFilePath, "pyarrow", index=False)

        self.remove()
        
    def remove(self) -> None:
        self.filePath.unlink()

    def getColumns(self) -> list[str]:
        df = self.read(nrows=1)
        return df.columns

class TSVSubfile(Subfile):

    fileFormat = Format.TSV

    def write(self, df: pd.DataFrame) -> None:
        df.to_csv(self.filePath, sep="\t", index=False)

    def read(self, **kwargs) -> pd.DataFrame:
        return pd.read_csv(self.filePath, sep="\t", **kwargs)
    
class PARQUETSubfile(Subfile):

    fileFormat = Format.PARQUET

    def write(self, df: pd.DataFrame) -> None:
        df.to_parquet(self.filePath, "pyarrow", index=False)

    def read(self, **kwargs) -> pd.DataFrame:
        # return pd.read_parquet(self.filePath, "pyarrow", **kwargs)
        return pq.read_table(self.filePath, **kwargs).to_pandas()
    
    def readChunks(self, chunkSize: int, **kwargs) -> Iterator[pd.DataFrame]:
        parquetFile = pq.ParquetFile(self.filePath)
        return (batch.to_pandas() for batch in parquetFile.iter_batches(batch_size=chunkSize, **kwargs))
    
    def getColumns(self) -> list[str]:
        pf = pq.read_schema(self.filePath)
        return pf.names

class BigFileWriter:
    def __init__(self, outputFile: Path, subDirName: str = "chunks", sectionPrefix: str = "chunk", subfileType: Format = Format.PARQUET) -> 'BigFileWriter':
        self.outputFile = outputFile
        self.outputFileType = Format(outputFile.suffix)
        self.subfileDir = outputFile.parent / subDirName
        self.sectionPrefix = sectionPrefix
        self.subfileType = subfileType

        self.writtenFiles: list[Subfile] = []
        self.globalColumns: list[str] = []

        maxInt = sys.maxsize
        while True:
            try:
                csv.field_size_limit(maxInt)
                return
            except OverflowError:
                maxInt = int(maxInt/10)

    def populateFromFolder(self, folderPath: Path) -> None:
        if not folderPath.exists():
            return
        
        for filePath in folderPath.iterdir():
            if not filePath.suffix in Format._value2member_map_.keys():
                continue

            subFile = Subfile.fromFilePath(filePath)
            columns = subFile.getColumns()

            self.writtenFiles.append(subFile)
            cmn.extendUnique(self.globalColumns, columns)

            Logger.info(f"Added file: {subFile.filePath}")

    def writeCSV(self, cols: list[str], rows: list[list[str]]) -> None:
        df = pd.DataFrame(columns=cols, data=rows)
        self.writeDF(df)

    def writeDF(self, df: pd.DataFrame, format: Format = None) -> None:
        if not self.subfileDir.exists():
            self.subfileDir.mkdir(parents=True)

        if format is None:
            format = self.subfileType

        subfile = Subfile(self.subfileDir, f"{self.sectionPrefix}_{len(self.writtenFiles)}", format)
        subfile.write(df)

        self.writtenFiles.append(subfile)
        self.globalColumns = cmn.extendUnique(self.globalColumns, df.columns)

    def oneFile(self, removeOld: bool = True) -> None:
        if self.outputFile.exists():
            Logger.info(f"Removing old file {self.outputFile}")
            self.outputFile.unlink()

        if len(self.writtenFiles) == 1:
            Logger.info(f"Only single subfile, moving {self.writtenFiles[0]} to {self.outputFile}")

            self.writtenFiles[0].rename(self.outputFile, self.outputFileType)
            self.subfileDir.rmdir()
            return

        Logger.info("Combining into one file")
        if self.outputFileType in (Format.CSV, Format.TSV):
            self._oneCSV(removeOld)
        elif self.outputFileType == Format.PARQUET:
            self._oneParquet(removeOld)

        Logger.info(f"\nCreated a single file at {self.outputFile}")
        if removeOld:
            self.subfileDir.rmdir()
            self.writtenFiles.clear()

    def _oneCSV(self, removeOld: bool = True):
        delim = "\t" if self.outputFileType == Format.TSV else ","
        chunkSize = 1024

        fileCount = len(self.writtenFiles)
        for idx, file in enumerate(self.writtenFiles):
            print(f"At file: {idx+1} / {fileCount}", end='\r')

            for subIdx, chunk in enumerate(file.readChunks(chunkSize)):
                if idx == subIdx == 0:
                    chunk.to_csv(self.outputFile, mode="a", sep=delim, index=False)
                    continue

                chunk.to_csv(self.outputFile, mode="a", sep=delim, index=False, header=False)

            if removeOld:
                file.remove()
        
    def _oneParquet(self, removeOld: bool = True):
        schema = pa.schema([(column, pa.string()) for column in self.globalColumns])
        with pq.ParquetWriter(self.outputFile, schema=schema) as writer:
            for file in self.writtenFiles:
                writer.write_table(pq.read_table(str(file)))

                if removeOld:
                    file.remove()
