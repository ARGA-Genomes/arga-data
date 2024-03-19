from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path
from enum import Enum
import pandas as pd
import lib.commonFuncs as cmn
from collections.abc import Iterator
from lib.tools.logger import Logger

if TYPE_CHECKING:
    from lib.processing.stageScript import StageScript, StageDWCConversion

class StageFileStep(Enum):
    DOWNLOADED   = 0
    PROCESSED    = 1
    PRE_DWC      = 2
    DWC          = 3
    INTERMEDIATE = 4

class StageFile:
    def __init__(self, filePath: Path, fileProperties: dict, parentScript: StageScript, stage: StageFileStep):
        self.filePath = filePath
        self.fileProperties = fileProperties.copy()
        self.parentScript = parentScript
        self.stage = stage
        self.directory = filePath.parent

        self.separator = fileProperties.pop("separator", ",")
        self.firstRow = fileProperties.pop("firstrow", 0)
        self.encoding = fileProperties.pop("encoding", "utf-8")

    def __repr__(self) -> str:
        return str(self.filePath)

    def getFilePath(self) -> Path:
        return self.filePath
    
    def exists(self) -> bool:
        return self.filePath.exists()
    
    def updateStage(self, stage: StageFileStep) -> None:
        self.stage = stage
    
    def loadDataFrame(self, offset: int = 0, rows: int = None) -> pd.DataFrame:
        return pd.read_csv(self.filePath, sep=self.separator, header=self.firstRow + offset, encoding=self.encoding, nrows=rows)
    
    def loadDataFrameIterator(self, chunkSize: int = 1024, offset: int = 0, rows: int = -1) -> Iterator[pd.DataFrame]:
        return cmn.chunkGenerator(self.filePath, chunkSize, self.separator, self.firstRow + offset, self.encoding, nrows=rows)

    def getColumns(self) -> list[str]:
        return cmn.getColumns(self.filePath, self.separator, self.firstRow)

    def create(self, overwriteStage: StageFileStep, overwriteAmount: int = 0, verbose: bool = False, **kwargs: dict) -> bool:
        if self.filePath.exists():

            # Valid overwrite if stage matches overwrite, stage is intermediate, or preDwC overwriting processing steps
            validOverwrite = self.stage in (overwriteStage, StageFileStep.INTERMEDIATE) or (overwriteStage == StageFileStep.PRE_DWC and self.stage == StageFileStep.PROCESSED)

            if not validOverwrite:
                return False
            
            if validOverwrite and overwriteAmount <= 0:
                Logger.info(f"{self.filePath} already exists")
                return False
        
        self.filePath.parent.mkdir(parents=True, exist_ok=True)
        self.parentScript.run(overwriteStage, overwriteAmount, verbose, **kwargs)
        return True

class StageDwCFile(StageFile):
    def __init__(self, filePath: Path, parentScript: StageDWCConversion):
        super().__init__(filePath, {}, parentScript, StageFileStep.DWC)
        
    def _getFiles(self) -> list[Path]:
        return [file for file in self.filePath.iterdir() if file.suffix == ".csv"]

    def loadDataFrame(self, offset: int = 0, rows: int = None) -> pd.DataFrame:
        dfs = {file.stem: pd.read_csv(file, header=offset, nrows=rows) for file in self._getFiles()}
        return pd.concat(dfs.values(), axis=1, keys=dfs.keys())
    
    def loadDataFrameIterator(self, chunkSize: int = 1024, offset: int = 0, rows: int = None) -> Iterator[pd.DataFrame]:
        sections = {file.stem: pd.read_csv(file, chunksize=chunkSize, header=offset, nrows=rows) for file in self._getFiles()}
        while True:
            try:
                yield pd.concat([next(chunk) for chunk in sections.values()], axis=1, keys=sections.keys())
            except StopIteration:
                return
