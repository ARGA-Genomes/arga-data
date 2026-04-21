from pathlib import Path
from lib.processing.files import DataFile
import logging
import importlib.util
import traceback
import sys
from functools import wraps

class FunctionScript:
    def __init__(self, modulePath: Path, functionName: str, libraryDirs: list[Path] = []):
        self.modulePath = modulePath
        self.functionName = functionName
        self.libraryDirs = libraryDirs

    def _importFunction(self) -> callable:
        pathExtension = [str(libraryPath.parent) for libraryPath in self.libraryDirs]
        sys.path.extend(pathExtension)

        spec = importlib.util.spec_from_file_location(self.modulePath.name, self.modulePath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, self.functionName)
    
    def _execute(self, processFunction: callable, verbose: bool, args: list = [], kwargs: dict = {}) -> tuple[bool, any]:
        if verbose:
            msg = f"Running {self.modulePath} function '{self.functionName}'"
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

    def run(self, verbose: bool, args: list = [], kwargs: dict = {}) -> tuple[bool, any]:
        try:
            processFunction = self._importFunction()
        except:
            logging.error(f"Error importing function '{self.functionName}' from path '{self.modulePath}'")
            logging.error(f"Import Error: {traceback.format_exc()}")
            return False, None

        return self._execute(processFunction, verbose, args, kwargs)

class OutputScript(FunctionScript):
    def __init__(self, modulePath: Path, functionName: str, outputDir: Path, inputs: list[DataFile] = [], libraryDirs: list[Path] = []):
        super().__init__(modulePath, functionName, libraryDirs)

        self.outputDir = outputDir
        self.inputs = inputs

    def _importFunction(self):
        processFunction = super()._importFunction()

        processAttributes = vars(processFunction)
        if "callable" not in processAttributes:
            logging.error(f"Function '{self.functionName}' from path '{self.modulePath}' is not properly decorated with the importableScript decorator in {Path(__file__)}")
            raise ImportError
        
        if not processAttributes["callable"]: # Decorator has no braces to execute outer decorator layer, call to expose proper imported function target
            processFunction = processFunction()

        self.ioArgStrt = processFunction.ioArgStart
        self.inputCount = processFunction.inputCount
        self.separateInputArgs = processFunction.separateInputArgs

        return processFunction

    def _execute(self, processFunction: callable, verbose: bool, args: list = [], kwargs: dict = {}) -> tuple[bool, any]:
        io = [self.outputDir]

        if self.inputCount != 0: # <0 for all inputs regardless of count, >0 for specific inputs checked above
            if self.inputCount > 0: # Selected quantity of inputs

                if self.inputCount > len(self.inputs):
                    if verbose:
                        logging.error(f"Imported function '{self.functionName}' from path '{self.modulePath}' expects {self.inputCount} inputs but {len(self.inputs)} were passed to it")
                        return False, None
                
                if self.inputCount < len(self.inputs):
                    if verbose:
                        logging.warning(f"Imported function '{self.functionName}' from path '{self.modulePath}' given {len(self.inputs)} inputs while only {self.inputCount} were expected. Running with first {self.inputCount} inputs only.")

                    self.inputs = self.inputs[:self.inputCount] # restrict excess provided inputs to inputCount

                if not all(input.exists() for input in self.inputs):
                    if verbose:
                        logging.warning(f"Missing {len(self.inputs)} required file(s) needed to run script {self.functionName}")

                    return False, None
            
            if self.separateInputArgs:
                io.extend(self.inputs)
            else:
                io.append(self.inputs)

        args = args[:self.ioArgStrt] + io + args[self.ioArgStrt:] # Inject io args at defined position
        return super()._execute(processFunction, verbose, args, kwargs)

def importableScript(ioArgStart: int = 0, inputCount: int = 1, separateInputArgs: bool = True):

    def scriptDecorator(func: callable):

        @wraps(func)
        def scriptWrapper(*args, **kwargs):
            return func(*args, **kwargs)
        
        scriptWrapper.ioArgStart = ioArgStart
        scriptWrapper.inputCount = inputCount
        scriptWrapper.separateInputArgs = separateInputArgs
        scriptWrapper.callable = True
        return scriptWrapper
    
    scriptDecorator.callable = False
    return scriptDecorator
