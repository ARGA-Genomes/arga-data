from pathlib import Path
from lib.processing.files import DataFile
import logging
import importlib.util
import traceback
import sys

class FunctionScript:
    def __init__(self, modulePath: Path, functionName: str, libraryDirs: list[Path] = []):
        self.modulePath = modulePath
        self.functionName = functionName
        self.libraryDirs = libraryDirs

    def _importFunction(self) -> callable:
        spec = importlib.util.spec_from_file_location(self.modulePath.name, self.modulePath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, self.functionName)

    def run(self, verbose: bool, args: list = [], kwargs: dict = {}) -> tuple[bool, any]:
        pathExtension = [str(libraryPath.parent) for libraryPath in self.libraryDirs]
        sys.path.extend(pathExtension)

        try:
            processFunction = self._importFunction()
        except:
            logging.error(f"Error importing function '{self.functionName}' from path '{self.modulePath}'")
            return False, None

        if verbose:
            msg = f"Running {self.modulePath} function '{self.function}'"
            if args:
                msg += f" with args {args}"
            if kwargs:
                if args:
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
    def __init__(self, modulePath: Path, functionName: str, outputs: list[DataFile], inputs: list[DataFile] = [], libraryDirs: list[Path] = []):
        super().__init__(modulePath, functionName, libraryDirs)

        self.outputs = outputs
        self.inputs = inputs

    def run(self, overwrite: bool, verbose: bool, args: list = [], kwargs: dict = {}) -> tuple[bool, any]:
        if all(output.exists() for output in self.outputs):
            if not overwrite:
                logging.info(f"All outputs for function '{self.functionName}' exist and not overwriting, skipping ...")
                return True, None
            
        if not all(input.exists() for input in self.inputs):
            logging.warning(f"Missing {len(self.inputs)} required file(s) needed to run script {self.functionName}")
            return False, None
            
        # All files don't exist and forced rerun OR overwriting
        for output in self.outputs:
            if output.exists():
                output.backUp(True)

        success, retVal = super().run(verbose, args, kwargs)

        if not success:
            for output in self.outputs:
                output.restoreBackUp()

            return False, retVal
        
        if not all(output.exists() for output in self.outputs):
            logging.warning(f"Output {self.outputs[0]} was not created" if len(self.outputs) == 1 else f"Failed to create all {len(self.outputs)} files")

            for output in self.outputs:
                output.restoreBackUp()

            return False, retVal
        
        logging.info(f"Created file {self.outputs[0]}" if len(self.outputs) == 1 else f"Created all {len(self.outputs)} files")
        for output in self.outputs:
            output.deleteBackup()

        return True, retVal
