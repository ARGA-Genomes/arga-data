import polars as pl
import lib.common as cmn
from pathlib import Path
from enum import Enum
import logging
from typing import Iterator
import shutil

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

    def move(self, newDir: Path) -> None:
        self.rename(newDir / self.path.name)

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

            self.properties[dataProperty.value] = value

    def read(self, **kwargs: dict) -> pl.DataFrame:
        raise NotImplementedError
    
    def scan(self, **kwargs: dict) -> pl.LazyFrame:
        raise NotImplementedError
    
    def write(self, df: pl.DataFrame, **kwargs: dict) -> None:
        raise NotImplementedError
    
    def sink(self, lf: pl.LazyFrame, **kwargs: dict) -> None:
        raise NotImplementedError
    
    def getSchema(self) -> pl.Schema:
        return self.scan().collect_schema()
    
class CSVFile(DataFile):
    
    format = DataFormat.CSV

    def read(self, **kwargs: dict) -> pl.DataFrame:
        return pl.read_csv(self.path, **(self.properties | kwargs))
    
    def scan(self, **kwargs: dict) -> pl.LazyFrame:
        return pl.scan_csv(self.path, **(self.properties | kwargs))
    
    def write(self, df: pl.DataFrame, **kwargs: dict) -> None:
        df.write_csv(self.path, **kwargs)

    def sink(self, lf: pl.LazyFrame, **kwargs: dict) -> None:
        lf.sink_csv(self.path, **kwargs)

class TSVFile(CSVFile, DataFile):
    
    format = DataFormat.TSV

    def __init__(self, path: Path, properties: dict = {}):
        super().__init__(path, {DataProperty.SEPERATOR.value: "\t"} | properties)

class ParquetFile(DataFile):
    
    format = DataFormat.PARQUET

    def read(self, **kwargs: dict) -> pl.DataFrame:
        return pl.read_parquet(self.path, **kwargs)
    
    def scan(self, **kwargs: dict) -> pl.LazyFrame:
        return pl.scan_parquet(self.path, **kwargs)
    
    def write(self, df: pl.DataFrame, **kwargs: dict) -> None:
        df.write_parquet(self.path, **kwargs)

    def sink(self, lf: pl.LazyFrame, **kwargs: dict) -> None:
        lf.sink_parquet(self.path, **kwargs)

class Folder(FileObject):
    def __init__(self, path: Path, create: bool = False):
        super().__init__(path)

        if create:
            path.mkdir(parents=True, exist_ok=True)

    def backUp(self, overwrite: bool = False) -> None:
        backupPath = self.path.parent / f"{self.path.stem}_backup{self.path.suffix}"
        if backupPath.exists():
            if not overwrite:
                logging.info("Unable to create new backup as it already exists")
                return
        
            cmn.clearFolder(backupPath, True)
        
        self._backupPath = shutil.move(self.path, backupPath)

    def restoreBackUp(self) -> None:
        if self._backupPath is None:
            return
        
        self.delete()
        shutil.move(self._backupPath, self.path)
        self._backupPath = None

    def delete(self) -> None:
        cmn.clearFolder(self.path, True)

    def deleteBackup(self) -> None:
        if self._backupPath is None:
            return

        cmn.clearFolder(self._backupPath, True)
        self._backupPath = None

class StackedFile(Folder, DataFile):

    format = DataFormat.STACKED

    def __init__(self, path: Path, create: bool = False, sectionFormat: DataFormat = DataFormat.CSV):
        super().__init__(path, create)

        self._sectionFormat = sectionFormat

    def _getFiles(self) -> list[DataFile]:
        return [dataFile for dataFile in [DataFile(file) for file in self.path.iterdir() if file.is_file()] if dataFile.format == self._sectionFormat]

    def read(self, **kwargs: dict) -> pl.DataFrame:
        dfs = {file.path.stem: file.read(**kwargs) for file in self._getFiles()}
        return pl.concat(dfs.values(), axis=1, keys=dfs.keys())
    
    def readIterator(self, chunkSize, **kwargs: dict) -> Iterator[pl.DataFrame]:
        sections = {file.path.stem: file.readIterator(chunkSize, **kwargs) for file in self._getFiles()}
        while True:
            try:
                yield pl.concat([next(chunk) for chunk in sections.values()], axis=1, keys=sections.keys())
            except StopIteration:
                return
            
    def write(self, df: pl.DataFrame, **kwargs: dict) -> None:
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
    outputFile.writeIterator(iterator, inputFile.getColumns(), index=False)
    inputFile.delete()
