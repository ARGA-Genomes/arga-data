from pathlib import Path
from lib.processing.files import DataFile, Folder
import logging
import importlib.util
from enum import Enum
import traceback
from lib.config import globalConfig as gcfg
from typing import Any
import sys

class FileSelect(Enum):
    INPUT    = "IN"
    OUTPUT   = "OUT"
    DOWNLOAD = "D"
    PROCESS  = "P"

class _FileProperty(Enum):
    DIR  = "DIR"
    FILE = "FILE"
    PATH = "PATH"

class FunctionScript:
    _libDir = gcfg.folders.src / "lib"

    def __init__(self, scriptDir: Path, scriptInfo: dict, libraryDirs: list[Path]):
        self.scriptDir = scriptDir
        self.scriptInfo = scriptInfo
        self.imports = {".lib": self._libDir} | {f".{libraryPath.name}": libraryPath for libraryPath in libraryDirs}

        # Script information
        modulePath: str = scriptInfo.pop("path", None)
        self.function: str = scriptInfo.pop("function", None)
        args: list[str] = scriptInfo.pop("args", [])
        kwargs: dict[str, str] = scriptInfo.pop("kwargs", {})

        if modulePath is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        self.modulePath = self._parsePath(modulePath, True)

        self.args = [self._parsePath(arg) for arg in args]
        self.kwargs = {key: self._parsePath(arg) for key, arg in kwargs.items()}

        for parameter in scriptInfo:
            logging.debug(f"Unknown script parameter: {parameter}")

    def _importFunction(self, modulePath: Path, functionName: str) -> callable:
        spec = importlib.util.spec_from_file_location(modulePath.name, modulePath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, functionName)

    def _parsePath(self, arg: Any, forceOutput: bool = False) -> Path | Any:
        if not isinstance(arg, str):
            return arg
        
        if arg.startswith("."):
            prefix, path = arg.split("/", 1)
            if prefix == ".":
                return self.scriptDir / path
            
            if prefix == "..":
                cwd = self.scriptDir.parent
                while path.startswith("../"):
                    cwd = cwd.parent
                    path = path[3:]

                return cwd / path
            
            if prefix in self.imports:
                return self.imports[prefix] / path

        if forceOutput:
            return Path(arg)
        
        return arg
    
    def run(self, verbose: bool, inputArgs: list = [], inputKwargs: dict = {}) -> tuple[bool, any]:
        try:
            pathExtension = [str(path.parent) for path in self.imports.values()]
            sys.path.extend(pathExtension)
            processFunction = self._importFunction(self.modulePath, self.function)
            sys.path = sys.path[:-len(pathExtension)]
        except:
            logging.error(f"Error importing function '{self.function}' from path '{self.modulePath}'")
            logging.error(traceback.format_exc())
            return False, None

        args = self.args + inputArgs
        kwargs = self.kwargs | inputKwargs

        if verbose:
            msg = f"Running {self.modulePath} function '{self.function}'"
            if self.args:
                msg += f" with args {args}"
            if self.kwargs:
                if self.args:
                    msg += " and"
                msg += f" with kwargs {kwargs}"
            logging.info(msg)
        
        try:
            retVal = processFunction(*args, **kwargs)
        except KeyboardInterrupt:
            logging.info("Cancelled external script")
            return False, None
        except PermissionError:
            logging.info("External script does not have permission to modify file, potentially open")
            return False, None
        except:
            logging.error(f"Error running external script:\n{traceback.format_exc()}")
            return False, None
                
        return True, retVal

class OutputScript(FunctionScript):
    fileLookup = {}
    
    def __init__(self, scriptDir: Path, scriptInfo: dict, outputDir: Path, libraryDirs: list[Path]):
        self.outputDir = outputDir

        # Output information
        outputName = scriptInfo.pop("output", None)
        outputProperties = scriptInfo.pop("properties", {})

        if outputName is None:
            raise Exception("No output specified, please use FunctionScript if intentionally has no output") from AttributeError

        self.output = self._parseOutput(outputName, outputProperties)
        self.fileLookup |= {FileSelect.OUTPUT: [self.output]}

        super().__init__(scriptDir, scriptInfo, libraryDirs)

        self.args = [self._parseArg(arg) for arg in self.args]
        self.kwargs = {key: self._parseArg(arg) for key, arg in self.kwargs.items()}

    def _parseOutput(self, outputName: str, outputProperties: dict) -> DataFile:
        return self._createFile(self.outputDir / outputName, outputProperties)

    def _createFile(self, outputPath: Path, outputProperties: dict) -> DataFile:
        if not outputPath.suffix:
            return Folder(outputPath)
        
        return DataFile(outputPath, outputProperties)
    
    def _parseArg(self, arg: Any) -> Path | str:
        if not isinstance(arg, str):
            return arg
        
        if not (arg.startswith("{") and arg.endswith("}")):
            return arg
        
        parsedArg = self._parseSelectorArg(arg[1:-1])
        if  isinstance(parsedArg, str):
            logging.warning(f"Unknown key code: {parsedArg}")
            return arg
        
        return parsedArg

    def _parseSelectorArg(self, argKey: str) -> Path | str:
        if "-" not in argKey:
            logging.warning(f"Both file type and file property not present in arg, deliminate with '-'")
            return argKey
        
        fType, fProperty = argKey.split("-")

        if fType[-1].isdigit():
            selection = int(fType[-1])
            fType = fType[:-1]
        else:
            selection = 0

        fTypeEnum = FileSelect._value2member_map_.get(fType, None)
        if fTypeEnum is None:
            logging.error(f"Invalid file type: '{file}'")
            return argKey

        files = self.fileLookup.get(fTypeEnum, None)
        if files is None:
            logging.error(f"No files provided for file type: '{fType}")
            return argKey

        if selection > len(files):
            logging.error(f"File selection '{selection}' out of range for file type '{fType}' which has a length of '{len(files)}")
            return argKey
        
        file: DataFile = files[selection]
        fProperty, *suffixes = fProperty.split(".")

        if fProperty == _FileProperty.FILE.value:
            if suffixes:
                logging.warning("Suffix provided for a file object which cannot be resolved, suffix not applied")
            return file
        
        if fProperty == _FileProperty.DIR.value:
            if suffixes:
                logging.warning("Suffix provided for a parent path which cannot be resolved, suffix not applied")
            return file.path.parent

        if fProperty == _FileProperty.PATH.value:
            pth = file.path
            for suffix in suffixes:
                pth = pth.with_suffix(suffix if not suffix else f".{suffix}") # Prepend a dot for valid suffixes
            return pth
        
        logging.error(f"Unable to parse file property: '{fProperty}")
        return argKey

    def run(self, overwrite: bool, verbose: bool, inputArgs: list = [], inputKwargs: dict = {}) -> tuple[bool, any]:
        if self.output.exists():
            if not overwrite:
                logging.info(f"Output {self.output} exist and not overwriting, skipping '{self.function}'")
                return True, None
            
            self.output.backUp(True)

        success, retVal = super().run(verbose, inputArgs, inputKwargs)
        if not success:
            self.output.restoreBackUp()
            return False, retVal
        
        if not self.output.exists():
            logging.warning(f"Output {self.output} was not created")
            self.output.restoreBackUp()
            return False, retVal
        
        logging.info(f"Created file {self.output}")
        self.output.deleteBackup()
        return True, retVal

class FileScript(OutputScript):
    def __init__(self, scriptDir: Path, scriptInfo: dict, outputDir: Path, inputs: dict[str, DataFile], imports: dict[str, Path] = {}):
        self.fileLookup |= inputs

        super().__init__(scriptDir, scriptInfo, outputDir, imports)

    def _parseOutput(self, outputName: str, outputProperties: dict) -> DataFile:
        parsedValue = self._parseArg(outputName)

        if isinstance(parsedValue, Path): # Redirect path of output to outputDir
            parsedValue = parsedValue.name

        return super()._parseOutput(parsedValue, outputProperties)
