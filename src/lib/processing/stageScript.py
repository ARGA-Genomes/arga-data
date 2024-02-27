from __future__ import annotations
from typing import TYPE_CHECKING
from pathlib import Path
from lib.processing.dwcProcessing import DWCProcessor
import lib.processing.processingFuncs as pFuncs
import lib.commonFuncs as cmn
from lib.tools.logger import Logger
from lib.processing.stageFile import StageFile, StageFileStep
from collections.abc import Iterable

if TYPE_CHECKING:
    from lib.processing.parser import SelectorParser

class StageScript:
    def __init__(self, processingStep: dict, inputs: list[StageFile], parser: SelectorParser):
        self.processingStep = processingStep.copy()
        self.inputs = inputs
        self.parser = parser
        self.scriptRun = False

        self.path = self.processingStep.pop("path", None)
        self.function = self.processingStep.pop("function", None)
        self.args = self.processingStep.pop("args", [])
        self.kwargs = self.processingStep.pop("kwargs", {})
        self.outputs = self.processingStep.pop("outputs", [])
        self.outputProperties = self.processingStep.pop("outputProperties", {})

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        self.args = self.parser.parseMultipleArgs(self.args, self.inputs)
        self.kwargs = {key: self.parser.parseArg(value, self.inputs) for key, value in self.kwargs.items()}
        self.outputs = self.parser.parseMultipleArgs(self.outputs, self.inputs)

        for parameter in self.processingStep:
            Logger.debug(f"Unknown step parameter: {parameter}")

    def getOutputs(self) -> list[tuple[Path, dict]]:
        return [(output, self.outputProperties.get(str(idx), {})) for idx, output in enumerate(self.outputs)]

    def run(self, overwriteStage: StageFileStep, overwrite: int = 0, verbose: bool = True, **kwargs: dict) -> tuple[bool, list]:
        if self.outputs and all(output.exists() for output in self.outputs) and overwrite <= 0:
            if verbose:
                Logger.info(f"All outputs {self.outputs} exist and not overwriting, skipping '{self.function}'")
            return (False, [])
        
        for input in self.inputs:
            input.create(overwriteStage, overwrite - 1, verbose, **kwargs)
            if not input.exists():
                if verbose:
                    Logger.info(f"Missing input file: {input.filePath}")
                return (False, [])
        
        if self.scriptRun:
            return (False, [])
        
        processFunction = pFuncs.importFunction(self.path, self.function)

        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if self.args:
                msg += f" with args {self.args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {self.kwargs}"
            print(msg)
        
        returnValue = processFunction(*self.args, **self.kwargs)

        # Make sure output is a list
        if not isinstance(returnValue, list): 
            returnValue = (returnValue,)
        elif isinstance(returnValue, tuple):
            returnValue = list(returnValue) # Unpack tuple to list

        self.scriptRun = True

        for output in self.outputs:
            if not output.exists():
                Logger.warning(f"Output {output} was not created")

        return (True, returnValue)

class StageDownloadScript:
    def __init__(self, url: str, downloadedFile: Path, parser: SelectorParser, user: str, password: str):
        self.url = url
        self.downloadedFile = downloadedFile
        self.parser = parser
        self.user = user
        self.password = password

        self.downloadedFile = parser.parseArg(downloadedFile, [])

    def getOutputs(self) -> list[Path]:
        return [self.downloadedFile]

    def run(self, overwriteStage: StageFileStep, overwriteAmount: int = 0, verbose: bool = False, **kwargs: dict):
        if self.downloadedFile.exists() and overwriteAmount <= 0:
            if verbose:
                Logger.info(f"File already downloaded at {self.downloadedFile}, skipping redownload")
            return
        
        cmn.downloadFile(self.url, self.downloadedFile, self.user, self.password, verbose)

class StageDWCConversion:
    def __init__(self, input: StageFile, dwcProcessor: DWCProcessor):
        self.input = input
        self.dwcProcessor = dwcProcessor
        self.outputFolderName = f"{self.input.filePath.stem}-dwc"

    def getOutput(self) -> Path:
        return self.dwcProcessor.outputDir / self.outputFolderName

    def run(self, overwriteStage: StageFileStep, overwriteAmount: int = 0, verbose: bool = True, **kwargs: dict):
        outputPath = self.getOutput()

        if outputPath.exists() and (overwriteAmount <= 0 or overwriteStage != StageFileStep.DWC):
            Logger.info(f"DWC file {outputPath} exists and not overwriting, skipping creation")
            return
        
        Logger.info(f"Creating DWC from preDWC file {self.input.filePath}")
        self.dwcProcessor.process(
            self.input.filePath,
            self.outputFolderName,
            self.input.separator,
            self.input.firstRow,
            self.input.encoding,
            overwriteAmount > 0,
            **kwargs
        )

class StageUpdateScript:
    def __init__(self, processingStep: dict, parser: SelectorParser):
        self.processingStep = processingStep.copy()
        self.parser = parser

        self.path = self.processingStep.pop("path", None)
        self.function = self.processingStep.pop("function", None)
        self.args = self.processingStep.pop("args", [])
        self.kwargs = self.processingStep.pop("kwargs", {})

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        self.args = self.parser.parseMultipleArgs(self.args, [])
        self.kwargs = {key: self.parser.parseArg(value, []) for key, value in self.kwargs.items()}

        for parameter in self.processingStep:
            Logger.debug(f"Unknown step parameter: {parameter}")

    def run(self, verbose: bool = False):
        processFunction = pFuncs.importFunction(self.path, self.function)

        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if self.args:
                msg += f" with args {self.args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {self.kwargs}"
            print(msg)

        return processFunction(*self.args, **self.kwargs)
