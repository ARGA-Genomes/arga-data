import pandas as pd
import lib.common as cmn
from pathlib import Path
from enum import Enum
from collections.abc import Iterator
import logging

class Step(Enum):
    DOWNLOADING = "downloading"
    PROCESSING  = "processing"
    CONVERSION  = "conversion"

class File:
    def __init__(self, filePath: Path, fileProperties: dict = {}):
        self.filePath = filePath
        self.fileProperties = fileProperties.copy()

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

        self._backupPath = None

    def __repr__(self) -> str:
        return str(self.filePath)
    
    def exists(self) -> bool:
        return self.filePath.exists()
    
    def backUp(self, overwrite: bool = False) -> None:
        newPath = self.filePath.parent / f"{self.filePath.stem}_backup{self.filePath.suffix}"
        if newPath.exists():
            if not overwrite:
                logging.info("Unable to create new backup as it already exists")
                return
        
            newPath.unlink()
        
        self._backupPath = self.filePath.rename(newPath)

    def restoreBackUp(self) -> None:
        if self._backupPath is None:
            return
        
        self.delete()
        self._backupPath.rename(self.filePath)
        self._backupPath = None
    
    def deleteBackup(self) -> None:
        if self._backupPath is None:
            return
        
        self._backupPath.unlink()
        self._backupPath = None
    
    def delete(self) -> None:
        self.filePath.unlink(True)
    
    def loadDataFrame(self, offset: int = 0, rows: int = None, **kwargs: dict) -> pd.DataFrame:
        return pd.read_csv(self.filePath, sep=self.separator, header=self.firstRow + offset, encoding=self.encoding, nrows=rows, **kwargs)
    
    def loadDataFrameIterator(self, chunkSize: int = 1024, offset: int = 0, rows: int = None) -> Iterator[pd.DataFrame]:
        return cmn.chunkGenerator(self.filePath, chunkSize, self.separator, self.firstRow + offset, self.encoding, nrows=rows)

    def getColumns(self) -> list[str]:
        return cmn.getColumns(self.filePath, self.separator, self.firstRow)

class Folder(File):
    def __init__(self, filePath: Path):
        super().__init__(filePath, {})

    def delete(self) -> None:
        cmn.clearFolder(self.filePath, True)

    def deleteBackup(self) -> None:
        if self._backupPath is None:
            return

        cmn.clearFolder(self._backupPath, True)
        self._backupPath = None

    def loadDataFrame(self, *args, **kwargs: dict) -> TypeError:
        raise TypeError
    
    def loadDataFrameIterator(self, *args, **kwargs) -> TypeError:
        return TypeError
    
    def getColumns(self) -> TypeError:
        return TypeError

class StackedFile(Folder):
    def _getFiles(self) -> list[Path]:
        return [file for file in self.filePath.iterdir() if file.suffix == ".csv"]

    def loadDataFrame(self, offset: int = 0, rows: int = None, **kwargs: dict) -> pd.DataFrame:
        dfs = {file.stem: pd.read_csv(file, sep=self.separator, header=offset, nrows=rows, encoding=self.encoding, **kwargs) for file in self._getFiles()}
        return pd.concat(dfs.values(), axis=1, keys=dfs.keys())
    
    def loadDataFrameIterator(self, chunkSize: int = 1024, offset: int = 0, rows: int = None) -> Iterator[pd.DataFrame]:
        sections = {file.stem: pd.read_csv(file, sep=self.separator, chunksize=chunkSize, header=offset, nrows=rows, encoding=self.encoding) for file in self._getFiles()}
        while True:
            try:
                yield pd.concat([next(chunk) for chunk in sections.values()], axis=1, keys=sections.keys())
            except StopIteration:
                return

    def getColumns(self) -> dict[str, list[str]]:
        return {file.stem: cmn.getColumns(file) for file in self._getFiles()}
