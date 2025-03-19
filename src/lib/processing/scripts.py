from pathlib import Path
from lib.processing.stages import File, Folder
from lib.tools.logger import Logger
import importlib.util
from enum import Enum
import traceback
import lib.config as cfg
from typing import Any

class FileSelect(Enum):
    INPUT    = "IN"
    OUTPUT   = "OUT"
    DOWNLOAD = "D"
    PROCESS  = "P"

class _FileProperty(Enum):
    DIR  = "DIR"
    FILE = "FILE"
    PATH = "PATH"
    STEM = "STEM"

class FunctionScript:
    _libDir = cfg.Folders.src / "lib"

    def __init__(self, baseDir: Path, scriptInfo: dict):
        self.baseDir = baseDir
        self.scriptInfo = scriptInfo

        # Script information
        self.path: str = scriptInfo.pop("path", None)
        self.function: str = scriptInfo.pop("function", None)
        self.args: list[str] = scriptInfo.pop("args", [])
        self.kwargs: dict[str, str] = scriptInfo.pop("kwargs", {})

        if self.path is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        self.path = self._parsePath(self.path, True)

        self.args = [self._parsePath(arg) for arg in self.args]
        self.kwargs = {key: self._parsePath(arg) for key, arg in self.kwargs.items()}

        for parameter in scriptInfo:
            Logger.debug(f"Unknown script parameter: {parameter}")

    def _importFunction(self, modulePath: Path, functionName: str) -> callable:
        spec = importlib.util.spec_from_file_location(modulePath.name, modulePath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, functionName)

    def _parsePath(self, arg: Any, forceOutput: bool = False) -> Path | Any:
        if not isinstance(arg, str):
            return arg
        
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
        
        if arg.startswith(".../"):
            return self._libDir / arg[4:]

        if forceOutput:
            return Path(arg)
        
        return arg
    
    def run(self, verbose: bool, inputArgs: list = [], inputKwargs: dict = {}) -> tuple[bool, any]:
        try:
            processFunction = self._importFunction(self.path, self.function)
        except:
            Logger.error(f"Error importing function '{self.function}' from path '{self.path}'")
            Logger.error(traceback.format_exc())
            return False, None

        args = self.args + inputArgs
        kwargs = self.kwargs | inputKwargs

        if verbose:
            msg = f"Running {self.path} function '{self.function}'"
            if self.args:
                msg += f" with args {args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {kwargs}"
            Logger.info(msg)
        
        try:
            retVal = processFunction(*args, **kwargs)
        except KeyboardInterrupt:
            Logger.info("Cancelled external script")
            return False, None
        except PermissionError:
            Logger.info("External script does not have permission to modify file, potentially open")
            return False, None
        except:
            Logger.error(f"Error running external script:\n{traceback.format_exc()}")
            return False, None
                
        return True, retVal

class OutputScript(FunctionScript):
    fileLookup = {}
    
    def __init__(self, baseDir: Path, scriptInfo: dict, outputDir: Path):
        self.outputDir = outputDir

        # Output information
        self.output = scriptInfo.pop("output", None)
        self.outputProperties = scriptInfo.pop("properties", {})

        if self.output is None:
            raise Exception("No output specified, please use FunctionScript if intentionally has no output") from AttributeError

        self.output = self._parseOutput(self.outputDir / self.output, self.outputProperties)
        self.fileLookup |= {FileSelect.OUTPUT: [self.output]}

        super().__init__(baseDir, scriptInfo)

        self.args = [self._parseArg(arg) for arg in self.args]
        self.kwargs = {key: self._parseArg(arg) for key, arg in self.kwargs.items()}

    def _parseOutput(self, outputPath: Path, outputProperties: dict) -> File:
        if not outputPath.suffix:
            return Folder(outputPath)
        return File(outputPath, outputProperties)
    
    def _parseArg(self, arg: Any) -> Path | str:
        if not isinstance(arg, str):
            return arg
        
        if not (arg.startswith("{") and arg.endswith("}")):
            return arg
        
        parsedArg = self._parseSelectorArg(arg[1:-1])
        if  isinstance(parsedArg, str):
            Logger.warning(f"Unknown key code: {parsedArg}")
            return arg
        
        return parsedArg

    def _parseSelectorArg(self, argKey: str) -> Path | str:
        if "-" not in argKey:
            Logger.warning(f"Both file type and file property not present in arg, deliminate with '-'")
            return argKey
        
        fType, fProperty = argKey.split("-")

        if fType[-1].isdigit():
            selection = int(fType[-1])
            fType = fType[:-1]
        else:
            selection = 0

        fTypeEnum = FileSelect._value2member_map_.get(fType, None)
        if fTypeEnum is None:
            Logger.error(f"Invalid file type: '{file}'")
            return argKey

        files = self.fileLookup.get(fTypeEnum, None)
        if files is None:
            Logger.error(f"No files provided for file type: '{fType}")
            return argKey

        if selection > len(files):
            Logger.error(f"File selection '{selection}' out of range for file type '{fType}' which has a length of '{len(files)}")
            return argKey
        
        file: File = files[selection]
        if fProperty == _FileProperty.DIR.value:
            return file.filePath.parent
        
        if fProperty == _FileProperty.FILE.value:
            return file
        
        if fProperty == _FileProperty.PATH.value:
            return file.filePath
        
        if fProperty == _FileProperty.STEM.value:
            return file.filePath.stem
        
        Logger.error(f"Unable to parse file property: '{fProperty}")
        return argKey

    def run(self, overwrite: bool, verbose: bool, inputArgs: list = [], inputKwargs: dict = {}) -> tuple[bool, any]:
        if self.output.exists():
            if not overwrite:
                Logger.info(f"Output {self.output} exist and not overwriting, skipping '{self.function}'")
                return True, None
            
            self.output.backUp(True)

        success, retVal = super().run(verbose, inputArgs, inputKwargs)
        if not success:
            self.output.restoreBackUp()
            return False, retVal
        
        if not self.output.exists():
            Logger.warning(f"Output {self.output} was not created")
            self.output.restoreBackUp()
            return False, retVal
        
        Logger.info(f"Created file {self.output}")
        self.output.deleteBackup()
        return True, retVal

class FileScript(OutputScript):
    def __init__(self, baseDir: Path, scriptInfo: dict, outputDir: Path, inputs: dict[str, File]):
        self.fileLookup |= inputs

        super().__init__(baseDir, scriptInfo, outputDir)

    def _parseOutput(self, output: str, outputProperties: dict) -> File:
        outputPath = self._parseArg(output)
        return super()._parseOutput(outputPath, outputProperties)
