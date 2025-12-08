from pathlib import Path
from lib.processing.files import DataFile, Folder
import logging
import importlib.util
import traceback
from lib.config import globalConfig as gcfg
import sys
import lib.processing.parsing as parse

class FunctionScript:
    _libDir = gcfg.folders.src / "lib"

    def __init__(self, scriptDir: Path, scriptInfo: dict, imports: list[str]):
        self.scriptDir = scriptDir
        self.scriptInfo = scriptInfo
        self.imports = imports + [self._libDir]

        # Script information
        modulePath: str = scriptInfo.pop("path", None)
        self.function: str = scriptInfo.pop("function", None)
        self.args: list[str] = scriptInfo.pop("args", [])
        self.kwargs: dict[str, str] = scriptInfo.pop("kwargs", {})

        if modulePath is None:
            raise Exception("No script path specified") from AttributeError
        
        if self.function is None:
            raise Exception("No script function specified") from AttributeError
        
        self.modulePath = parse.parsePath(modulePath, self.scriptDir)
        self.dirLookup = parse.DirLookup(imports)
        self.dataFileLookup = parse.DataFileLookup()

        for parameter in scriptInfo:
            logging.debug(f"Unknown script parameter: {parameter}")

    def _importFunction(self, modulePath: Path, functionName: str) -> callable:
        spec = importlib.util.spec_from_file_location(modulePath.name, modulePath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, functionName)
    
    def run(self, verbose: bool, inputArgs: list = [], inputKwargs: dict = {}) -> tuple[bool, any]:
        try:
            pathExtension = [str(path.parent) for path in self.imports]
            sys.path.extend(pathExtension)
            processFunction = self._importFunction(self.modulePath, self.function)
        except:
            logging.error(f"Error importing function '{self.function}' from path '{self.modulePath}'")
            logging.error(traceback.format_exc())
            return False, None

        args = [parse.parseArg(arg, self.scriptDir, self.dirLookup, self.dataFileLookup) for arg in self.args] + inputArgs
        kwargs = {key: parse.parseArg(value, self.scriptDir, self.dirLookup, self.dataFileLookup) for key, value in self.kwargs.items()} | inputKwargs

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
            sys.path = sys.path[:-len(pathExtension)]
            return False, None
        except PermissionError:
            logging.info("External script does not have permission to modify file, potentially open")
            sys.path = sys.path[:-len(pathExtension)]
            return False, None
        except:
            logging.error(f"Error running external script:\n{traceback.format_exc()}")
            sys.path = sys.path[:-len(pathExtension)]
            return False, None
        
        sys.path = sys.path[:-len(pathExtension)]
        return True, retVal

class OutputScript(FunctionScript):
    def __init__(self, scriptDir: Path, scriptInfo: dict, outputDir: Path, imports: list[Path] = []):
        self.outputDir = outputDir

        # Output information
        outputName = scriptInfo.pop("output", None)
        outputProperties = scriptInfo.pop("properties", {})

        if outputName is None:
            raise Exception("No output specified, please use FunctionScript if intentionally has no output") from AttributeError

        self.output = self._resolveOutput(outputName, outputProperties)

        super().__init__(scriptDir, scriptInfo, imports)

        self.dataFileLookup.merge(parse.DataFileLookup(outputs=[self.output]))

    def _resolveOutput(self, outputName: str, outputProperties: dict) -> DataFile:
        return self._createFile(self.outputDir / outputName, outputProperties)

    def _createFile(self, outputPath: Path, outputProperties: dict) -> DataFile:
        if not outputPath.suffix:
            return Folder(outputPath)
        
        return DataFile(outputPath, outputProperties)

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
    def __init__(self, scriptDir: Path, scriptInfo: dict, outputDir: Path, inputs: parse.DataFileLookup, imports: list[Path] = []):
        super().__init__(scriptDir, scriptInfo, outputDir, imports)

        self.dataFileLookup.merge(inputs)

    def _resolveOutput(self, outputName: str, outputProperties: dict) -> DataFile:
        parsedValue = parse.parseArg(outputName, self.outputDir, self.dirLookup, self.dataFileLookup)

        if isinstance(parsedValue, Path): # Redirect path of output to outputDir
            parsedValue = parsedValue.name

        return super()._resolveOutput(parsedValue, outputProperties)
