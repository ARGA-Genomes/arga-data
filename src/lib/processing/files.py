import pandas as pd
import lib.common as cmn
from pathlib import Path
from enum import Enum
import logging
import pyarrow.parquet as pq
from typing import Iterator
import pyarrow as pa

class Step(Enum):
    DOWNLOADING = "downloading"
    PROCESSING  = "processing"
    CONVERSION  = "conversion"

class DataFormat(Enum):
    CSV     = ".csv"
    TSV     = ".tsv"
    PARQUET = ".parquet"
    STACKED = ""
    UNKNOWN = None

class DataProperty(Enum):
    SEPERATOR = "sep"
    ENCODING  = "encoding"
    HEADER    = "header"

class FileObject:
    def __init__(self, path: Path):
        self.path = path
        self._backupPath = None

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        return str(self)
    
    def exists(self) -> bool:
        return self.path.exists()
    
    def backUp(self, overwrite: bool = False) -> None:
        backupPath = self.path.parent / f"{self.path.stem}_backup{self.path.suffix}"
        if backupPath.exists():
            if not overwrite:
                logging.info("Unable to create new backup as it already exists")
                return
        
            backupPath.unlink()
        
        self._backupPath = self.path.rename(backupPath)

    def restoreBackUp(self) -> None:
        if self._backupPath is None:
            return
        
        self.delete()
        self._backupPath.rename(self.path)
        self._backupPath = None
    
    def deleteBackup(self) -> None:
        if self._backupPath is None:
            return
        
        self._backupPath.unlink()
        self._backupPath = None
    
    def delete(self) -> None:
        self.path.unlink(True)

    def rename(self, newPath: Path) -> None:
        self.path.rename(newPath)

class DataFile(FileObject):

    format = DataFormat.UNKNOWN

    def __new__(cls, *args):
        subclassMap = {subclass.format: subclass for subclass in cls.__subclasses__()}
        subclass = DataFormat._value2member_map_.get(Path(args[0]).suffix, None)
        if subclass is None or subclass not in subclassMap:
            return super().__new__(cls)
        
        return super().__new__(subclassMap[subclass])

    def __init__(self, path: Path, properties: dict = {}):
        super().__init__(path)

        self.properties: dict[DataProperty, any] = {}
        for property, value in properties.items():
            dataProperty = DataProperty._value2member_map_.get(property, None)
            if dataProperty is None:
                logging.warning(f"Unknown data file property: {property}")
                continue

            self.properties[dataProperty] = value

    def read(self, **kwargs: dict) -> pd.DataFrame:
        raise NotImplementedError
    
    def readIterator(self, chunkSize: int, **kwargs: dict) -> Iterator[pd.DataFrame]:
        raise NotImplementedError
    
    def write(self, df: pd.DataFrame, **kwargs: dict) -> None:
        raise NotImplementedError
    
    def writeIterator(self, iterator: Iterator[pd.DataFrame], columns: list[str], **kwargs: dict) -> None:
        raise NotImplementedError
    
    def getColumns(self) -> list[str]:
        raise NotImplementedError
    
class CSVFile(DataFile):
    
    format = DataFormat.CSV

    def read(self, **kwargs: dict) -> pd.DataFrame:
        return pd.read_csv(self.path, **(self.properties | kwargs))
    
    def readIterator(self, chunkSize: int, **kwargs) -> Iterator[pd.DataFrame]:
        return self.read(chunkSize=chunkSize, **kwargs)
    
    def write(self, df: pd.DataFrame, **kwargs: dict) -> None:
        df.to_csv(self.path, **kwargs)

    def writeIterator(self, iterator: Iterator[pd.DataFrame], columns: list[str], **kwargs: dict) -> None:
        for idx, chunk in enumerate(iterator):
            self.write(chunk, header=columns if idx == 0 else False, mode="a", **kwargs)

    def getColumns(self) -> list[str]:
        df = self.read(nrows=1)
        if df is None:
            return []
        return list(df.columns)

class TSVFile(CSVFile, DataFile):
    
    format = DataFormat.TSV

    def __init__(self, path: Path, properties: dict = {}):
        super().__init__(path, {DataProperty.SEPERATOR: "\t"} | properties)

class ParquetFile(DataFile):
    
    format = DataFormat.PARQUET

    def read(self, **kwargs: dict) -> pd.DataFrame:
        return pq.read_table(self.path, **kwargs).to_pandas()
    
    def readIterator(self, chunkSize: int, **kwargs) -> Iterator[pd.DataFrame]:
        parquetFile = pq.ParquetFile(self.path)
        return (batch.to_pandas() for batch in parquetFile.iter_batches(batch_size=chunkSize, **kwargs))

    def write(self, df: pd.DataFrame, **kwargs: dict) -> None:
        df.to_parquet(self.path, "pyarrow")

    def writeIterator(self, iterator: Iterator[pd.DataFrame], columns: list[str], **kwargs: dict) -> None:
        schema = pa.schema([(column, pa.string()) for column in columns])
        with pq.ParquetWriter(self.path, schema=schema) as writer:
            for chunk in iterator:
                writer.write_table(chunk)

    def getColumns(self) -> list[str]:
        pf = pq.read_schema(self.path)
        return pf.names

class Folder(FileObject):
    def __init__(self, path: Path, create: bool = False):
        super().__init__(path)

        if create:
            path.mkdir(exist_ok=True)

    def delete(self) -> None:
        cmn.clearFolder(self.path, True)

    def deleteBackup(self) -> None:
        if self._backupPath is None:
            return

        cmn.clearFolder(self._backupPath, True)
        self._backupPath = None

    def getMatchingPaths(self, pattern: str) -> Iterator[Path]:
        return self.path.glob(pattern)

class StackedFile(Folder, DataFile):

    format = DataFormat.STACKED

    def __init__(self, path: Path, create: bool = False, sectionFormat: DataFormat = DataFormat.CSV):
        super().__init__(path, create)

        self._sectionFormat = sectionFormat

    def _getFiles(self) -> list[DataFile]:
        return [DataFile(file) for file in self.path.iterdir()]

    def read(self, **kwargs: dict) -> pd.DataFrame:
        dfs = {file.path.stem: file.read(**kwargs) for file in self._getFiles()}
        return pd.concat(dfs.values(), axis=1, keys=dfs.keys())
    
    def readIterator(self, chunkSize, **kwargs: dict) -> Iterator[pd.DataFrame]:
        sections = {file.path.stem: file.readIterator(chunkSize, **kwargs) for file in self._getFiles()}
        while True:
            try:
                yield pd.concat([next(chunk) for chunk in sections.values()], axis=1, keys=sections.keys())
            except StopIteration:
                return
            
    def write(self, df: pd.DataFrame, **kwargs: dict) -> None:
        for outerColumn in df.columns.levels[0]:
            dataFile = DataFile(self.path / f"{outerColumn}{self._sectionFormat.value}")
            dataFile.write(df[outerColumn])

    def getColumns(self) -> dict[str, list[str]]:
        return {file.path.stem: file.getColumns() for file in self._getFiles()}

def moveDataFile(inputFile: DataFile, outputFile: DataFile):
    if inputFile.format == outputFile.format:
        inputFile.rename(outputFile.path)
        inputFile.delete()
        return
    
    iterator = inputFile.readIterator(1024 * 16)
    outputFile.writeIterator(iterator, index=False)
    inputFile.delete()

def combinedIterator(dataFiles: list[DataFile], chunkSize: int, **kwargs: dict) -> Iterator:
    return (chunk for file in dataFiles for chunk in file.readIterator(chunkSize, **kwargs))
