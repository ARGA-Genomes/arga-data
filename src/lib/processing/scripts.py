from pathlib import Path
from lib.processing.stages import File, Folder
from lib.tools.logger import Logger
import importlib.util
from enum import Enum
import traceback
import lib.config as cfg

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

        for parameter in scriptInfo:
            Logger.debug(f"Unknown script parameter: {parameter}")

    def _importFunction(self, modulePath: Path, functionName: str) -> callable:
        spec = importlib.util.spec_from_file_location(modulePath.name, modulePath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, functionName)

    def _parsePath(self, arg: str, forceOutput: bool = False) -> Path | str:
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

        Logger.warning(f"Unable to parse suspected path: {arg}")
        if forceOutput:
            return Path(arg)
        
        return arg
    
    def run(self, verbose: bool, inputArgs: list = [], inputKwargs: dict = {}) -> tuple[bool, any]:
        try:
            processFunction = self._importFunction(self.path, self.function)
        except:
            Logger.error(f"Error importing function '{self.function}' from path '{self.path}'")
            Logger.error(traceback.format_exc())
            if isinstance(self.output, File):
                self.output.restoreBackUp()
            return False

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
    def __init__(self, baseDir: Path, scriptInfo: dict, outputDir: Path):
        self.outputDir = outputDir

        # Output information
        self.output = scriptInfo.pop("output", None)
        self.outputProperties = scriptInfo.pop("properties", {})

        if self.output is None:
            raise Exception("No output specified, please use FunctionScript if intentionally has no output") from AttributeError

        self.output = self._parseOutput(self.outputDir / self.output, self.outputProperties)

        super().__init__(baseDir, scriptInfo)

    def _parseOutput(self, outputPath: Path, outputProperties: dict) -> File:
        if not outputPath.suffix:
            return Folder(outputPath)
        return File(outputPath, outputProperties)

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
    class _Key(Enum):
        INPUT_FILE  = "INFILE"
        INPUT_PATH  = "INPATH"
        INPUT_STEM  = "INSTEM"
        INPUT_DIR   = "INDIR"
        OUTPUT_FILE = "OUTFILE"
        OUTPUT_DIR  = "OUTDIR"
        OUTPUT_PATH = "OUTPATH"

    def __init__(self, baseDir: Path, scriptInfo: dict, outputDir: Path, inputs: list[File]):
        self.inputs = inputs

        super().__init__(baseDir, scriptInfo, outputDir)

        self.args = [self._parseArg(arg) for arg in self.args]
        self.kwargs = {key: self._parseArg(arg) for key, arg in self.kwargs.items()}

    def _parseOutput(self, output: str, outputProperties: dict) -> File:
        outputPath = self._parseArg(output, [self._Key.OUTPUT_DIR, self._Key.OUTPUT_PATH])
        return super()._parseOutput(outputPath, outputProperties)

    def _parseArg(self, arg: any, excludeKeys: list[_Key] = []) -> Path | str:
        if not isinstance(arg, str):
            return arg
        
        if arg.startswith("."):
            arg = self._parsePath(arg)
            if isinstance(arg, str):
                Logger.warning(f"Argument {arg} starts with '.' but is not a path")
                
            return arg
        
        if not (arg.startswith("{") and arg.endswith("}")):
            return arg
        
        argValue = arg[1:-1].split("_")
        if len(argValue) == 1:
            selection = 0
        elif len(argValue) == 2:
            if argValue[1].isdigit():
                selection = int(argValue[1])
            else:
                Logger.warning(f"Invalid selection number: {argValue[1]}")
                return arg
        else:
            Logger.warning(f"Cannot interpret input: {arg}")
            return arg

        argValue = argValue[0]
        if argValue not in self._Key._value2member_map_:
            Logger.warning(f"Unknown key code: {argValue}")
            return arg
        
        key = self._Key._value2member_map_[argValue]
        if key in excludeKeys:
            Logger.warning(f"Disallowed key code: {argValue}")
            return arg
        
        # Parsing key
        if key == self._Key.OUTPUT_FILE:
            return self.output
            
        if key == self._Key.OUTPUT_DIR:
            return self.outputDir
        
        if key == self._Key.OUTPUT_PATH:
            if not isinstance(self.output, File):
                Logger.warning("No output path found")
                return None
            
            return self.output.filePath

        if key == self._Key.INPUT_DIR:
            if not self.inputs:
                Logger.warning("No inputs to get directory from")
                return None
            
            return self.inputs[selection].filePath.parent
        
        if key in (self._Key.INPUT_FILE, self._Key.INPUT_PATH, self._Key.INPUT_STEM):
            if not self.inputs:
                Logger.warning("No inputs to get path from")
                return None
            
            file = self.inputs[selection]
            if key == self._Key.INPUT_FILE:
                return file

            path = file.filePath
            if key == self._Key.INPUT_PATH:
                return path
            
            return path.stem
