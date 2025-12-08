from pathlib import Path
from lib.processing.files import Step
from lib.systemManagers.baseManager import SystemManager, Task
import lib.processing.parsing as parse
import logging
import lib.zipping as zp
from lib.config import globalConfig as gcfg
from datetime import datetime

class _Collection(Task):
    def __init__(self, baseDir: Path, workingDir: Path, files: list[str], name: str):
        super().__init__()

        self.baseDir = baseDir
        self.workingDir = workingDir
        self.files = files
        self.name = name

        outputDir = gcfg.overwrites.package if gcfg.overwrites.package else self.baseDir
        self.outputPath = outputDir /  f"{self.name}_{datetime.now().date()}"

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        subFolder = self.workingDir
        subFolder.mkdir(parents=True, exist_ok=True)

        compressedPath = None
        moved: dict[str, dict[Path, Path]] = {}

        for file in self.files:
            filePath = parse.parsePath(file, self.baseDir)
            if not filePath.exists():
                logging.error(f"Unable to find file: {filePath}")
                break
            
            fileName = filePath.name
            oldParent = filePath.parent
            
            newPath = filePath.rename(subFolder / fileName)
            moved[fileName] = (newPath, oldParent)
        else:
            compressedPath = zp.compress(subFolder, self.outputPath.parent, self.outputPath.name, includeFolder=False)
            logging.info(f"Created zip file at: {compressedPath}")

        for fileName, (newPath, oldParent) in moved.items():
            newPath.rename(oldParent / fileName)
        
        subFolder.rmdir()
        return compressedPath is not None
    
    def getOutputPath(self) -> Path:
        return self.outputPath

class CompilationManager(SystemManager):
    def __init__(self, dataDir: Path, metadataDir: Path, name: str):
        super().__init__(dataDir, metadataDir, Step.COMPILING, "build")

        self.name = name

    def prepareFiles(self, files: list[str]) -> None:
        self._tasks.append(_Collection(self.metadataDir, self.workingDir, files, self.name))

    def compile(self, overwrite: bool, verbose: bool) -> bool:
        return self.runTasks(overwrite, verbose)
