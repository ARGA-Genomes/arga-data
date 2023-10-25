from pathlib import Path
from lib.processing.stageFile import StageFileStep, StageFile
from lib.processing.stageScript import StageDownloadScript, StageScript, StageDWCConversion
from lib.processing.parser import SelectorParser
from lib.processing.dwcProcessing import DWCProcessor
from copy import deepcopy
import concurrent.futures

class SystemManager:
    def __init__(self, location: str, rootDir: Path, dwcProperties: dict, enrichDBs: dict, authFileName: str = ""):
        self.location = location
        self.rootDir = rootDir
        self.authFileName = authFileName
        self.dwcProperties = dwcProperties
        self.enrichDBs = enrichDBs

        self.user = ""
        self.password = ""

        if self.authFileName:
            with open(self.rootDir / self.authFileName) as fp:
                data = fp.read().splitlines()

            self.user = data[0].split('=')[1]
            self.password = data[1].split('=')[1]

        self.dataDir = self.rootDir / "data"
        self.downloadDir = self.dataDir / "raw"
        self.processingDir = self.dataDir / "processing"
        self.preConversionDir = self.dataDir / "preConversion"
        self.dwcDir = self.dataDir / "dwc"
        
        self.parser = SelectorParser(self.rootDir, self.dataDir, self.downloadDir, self.processingDir, self.preConversionDir, self.dwcDir)
        self.dwcProcessor = DWCProcessor(self.location, self.dwcProperties, self.dwcDir)

        self.stageFiles: dict[StageFileStep, list[StageFile]] = {stage: [] for stage in StageFileStep}

    def getFiles(self, stage: StageFileStep) -> list[StageFile]:
        return self.stageFiles[stage]
    
    def create(self, stage: StageFileStep, fileNumbers: list[int] = [], overwrite: int = 0, maxWorkers: int = 100) -> bool:
        files: list[StageFile] = []
        
        if not fileNumbers: # Create all files:
            files = self.stageFiles[stage] # All stage files
        else:
            for number in fileNumbers:
                if number >= 0 and number <= len(self.stageFiles[stage]):
                    files.append(self.stageFiles[stage][number])
                else:
                    print(f"Invalid number provided: {number}")

        if len(files) <= 10: # Skip threadpool if only 1 file being processed
            return any(file.create(stage, overwrite) for file in files) # Return if any files were created

        print("Using concurrency for large quantity of tasks")
        createdFile = False # Check if any new files were actually created
        with concurrent.futures.ThreadPoolExecutor(max_workers=maxWorkers) as executor:
            futures = (executor.submit(file.create, (stage, overwrite)) for file in files)
            try:
                for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                    success = future.result()

                    if success:
                        print(f"Created file: {idx} of {len(files)}", end="\r")
                        createdFile = True
                    
            except KeyboardInterrupt:
                print("\nExiting...")
                executor.shutdown(cancel_futures=True)
                return False
            
            print()

        return createdFile

    def buildProcessingChain(self, processingSteps: list[dict], initialInputs: list[StageFile], finalStage: StageFileStep) -> None:
        inputs = initialInputs.copy()
        for idx, step in enumerate(processingSteps, start=1):
            scriptStep = StageScript(step, inputs, self.parser)
            stage = StageFileStep.INTERMEDIATE if idx < len(processingSteps) else finalStage
            inputs = [StageFile(filePath, properties, scriptStep, stage) for filePath, properties in scriptStep.getOutputs()]

        self.stageFiles[finalStage].extend(inputs)

    def addDownloadURLStage(self, url: str, fileName: str, processing: list[dict], fileProperties: dict = {}, buildProcessing: bool =True):
        downloadedFile = self.downloadDir / fileName # downloaded files go into download directory
        downloadScript = StageDownloadScript(url, downloadedFile, self.parser, self.user, self.password)
        
        rawFile = StageFile(downloadedFile, fileProperties.copy(), downloadScript, StageFileStep.DOWNLOADED)
        self.stageFiles[StageFileStep.DOWNLOADED].append(rawFile)

        if buildProcessing:
            self.buildProcessingChain(processing, [rawFile], StageFileStep.PROCESSED)

    def addRetrieveScriptStage(self, script: dict, processing: list[dict], buildProcessing: bool = True):
        scriptStep = StageScript(script, [], self.parser)
        outputs = [StageFile(filePath, properties, scriptStep, StageFileStep.DOWNLOADED) for filePath, properties in scriptStep.getOutputs()]
        self.stageFiles[StageFileStep.DOWNLOADED].extend(outputs)

        if not buildProcessing:
            self.buildProcessingChain(processing, outputs, StageFileStep.PROCESSED)
    
    def addFinalStage(self, processing):
        self.buildProcessingChain(processing, self.stageFiles[StageFileStep.PROCESSED], StageFileStep.PRE_DWC)
    
    def prepareDwC(self):
        for file in self.stageFiles[StageFileStep.PRE_DWC]:
            conversionScript = StageDWCConversion(file, self.dwcProcessor)
            dwcOutput = conversionScript.getOutput()
            convertedFile = StageFile(dwcOutput, {}, conversionScript, StageFileStep.DWC)
            self.stageFiles[StageFileStep.DWC].append(convertedFile)
