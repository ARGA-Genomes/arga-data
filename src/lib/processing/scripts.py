from pathlib import Path
from lib.processing.stages import File
from lib.tools.logger import Logger
import lib.processing.processingFuncs as pFuncs
from enum import Enum

class Key(Enum):
    INPUT_FILE  = "INFILE"
    INPUT_PATH  = "INPATH"
    INPUT_STEM  = "INSTEM"
    INPUT_DIR   = "INDIR"
    OUTPUT_FILE = "OUTFILE"
    OUTPUT_DIR  = "OUTDIR"
    OUTPUT_PATH = "OUTPATH"

class Script:
    def __init__(self, baseDir: Path, outputDir: Path, scriptInfo: dict, inputs: list[File]):
        self.baseDir = baseDir
        self.outputDir = outputDir
        self.inputs = inputs

        # Script information
        self.path: str = scriptInfo.pop("path", None)
        self.function: str = scriptInfo.pop("function", None)
        self.args: list[str] = scriptInfo.pop("args", [])
        self.kwargs: dict[str, str] = scriptInfo.pop("kwargs", {})

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        # Output information
        self.output = scriptInfo.pop("output", "")
        self.outputProperties = scriptInfo.pop("properties", {})

        for parameter in scriptInfo:
            Logger.debug(f"Unknown step parameter: {parameter}")

        # Parsing
        self.output = self._parseArg(self.output, [Key.OUTPUT_DIR, Key.OUTPUT_PATH])
        if isinstance(self.output, str):
            self.output = self.outputDir / self.output
        self.output: File = File(self.output, self.outputProperties)

        self.args = [self._parseArg(arg) for arg in self.args]
        self.kwargs = {key: self._parseArg(arg) for key, arg in self.kwargs.items()}

    def run(self, overwrite: bool = False, verbose: bool = False, **runtimeKwargs: dict) -> any:
        if isinstance(self.output, Path) and self.output.exists():
            if not overwrite:
                Logger.info(f"Output {self.output} exist and not overwriting, skipping '{self.function}'")
                return
            
            self.output.delete()

        processFunction = pFuncs.importFunction(self.path, self.function)

        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if self.args:
                msg += f" with args {self.args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {self.kwargs}"
            Logger.info(msg)

        output = processFunction(*self.args, **self.kwargs)

        if not self.output.exists():
            Logger.warning(f"Output {self.output} was not created")
        else:
            Logger.info(f"Created file {self.output}")
            
        return output

    def _parseArg(self, arg: any, excludeKeys: list[Key] = []) -> Path | str:
        if not isinstance(arg, str):
            return arg
        
        if arg.startswith("."):
            arg = self._parsePath(arg)
            if isinstance(arg, str):
                Logger.warning(f"Argument {arg} starts with '.' but is not a path")
                
            return arg
        
        if not (arg.startswith("{") and arg.endswith("}")):
            return arg
        
        argValue = arg[1:-1]
        if argValue not in Key._value2member_map_:
            Logger.warning(f"Unknown key code: {argValue}")
            return arg
        
        key = Key._value2member_map_[argValue]
        if key in excludeKeys:
            Logger.warning(f"Disallowed key code: {argValue}")
            return arg
        
        # Parsing key
        if key == Key.OUTPUT_FILE:
            return self.output
            
        if key == Key.OUTPUT_DIR:
            return self.outputDir
        
        if key == Key.OUTPUT_PATH:
            if not isinstance(self.output, File):
                Logger.warning("No output path found")
                return None
            
            return self.output.filePath

        if key == Key.INPUT_DIR:
            if not self.inputs:
                Logger.warning("No inputs to get directory from")
                return None
            
            return self.inputs[0].filePath.parent
        
        if key in (Key.INPUT_FILE, Key.INPUT_PATH, Key.INPUT_STEM):
            if not self.inputs:
                Logger.warning("No inputs to get path from")
                return None
            
            if len(self.inputs) > 1:
                Logger.warning("Too many inputs to get path from")
                return None
            
            if key == Key.INPUT_FILE:
                return self.inputs[0]

            if key == Key.INPUT_PATH:
                return self.inputs[0].filePath
            
            return self.inputs[0].filePath.stem

    def _parsePath(self, arg: str) -> Path | str:
        if arg.startswith("./"):
            workingDir = self.baseDir
            return workingDir / arg[2:]
         
        if arg.startswith("../"):
            workingDir = self.baseDir.parent
            newStructure = arg[3:]
            while newStructure.startswith("../"):
                workingDir = workingDir.parent
                newStructure = newStructure[3:]

            return workingDir / newStructure
        
        return arg
