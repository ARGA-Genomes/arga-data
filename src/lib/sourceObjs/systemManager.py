from pathlib import Path
from lib.processing.stageFile import StageFileStep, StageFile
from lib.processing.stageScript import StageDownloadScript, StageScript, StageDWCConversion
from lib.processing.parser import SelectorParser
from lib.processing.dwcProcessor import DWCProcessor
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

        self.stages: dict[StageFileStep, list[StageFile]] = {stage: [] for stage in StageFileStep}

    def getFiles(self, stage: StageFileStep) -> list[StageFile]:
        return self.stages[stage]
    
    def create(self, stage: StageFileStep, fileNumbers: list[int] = [], overwrite: int = 0, maxWorkers: int = 100) -> bool:
        files: list[StageFile] = []
        
        if not fileNumbers: # Create all files:
            files = self.stages[stage] # All stage files
        else:
            for number in fileNumbers:
                if number >= 0 and number <= len(self.stages[stage]):
                    files.append(self.stages[stage][number])
                else:
                    print(f"Invalid number provided: {number}")

        print(files)
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

        self.stages[finalStage].extend(inputs)

    def addDownloadURLStage(self, url: str, fileName: str, processing: list[dict], fileProperties: dict = {}):
        downloadedFile = self.downloadDir / fileName # downloaded files go into download directory
        downloadScript = StageDownloadScript(url, downloadedFile, self.parser, self.user, self.password)
        
        rawFile = StageFile(downloadedFile, fileProperties.copy(), downloadScript, StageFileStep.RAW)
        self.stages[StageFileStep.RAW].append(rawFile)

        self.buildProcessingChain(processing, [rawFile], StageFileStep.PROCESSED)

    def addRetrieveScriptStage(self, script, processing):
        scriptStep = StageScript(script, [], self.parser)
        outputs = [StageFile(filePath, properties, scriptStep, StageFileStep.RAW) for filePath, properties in scriptStep.getOutputs()]
        self.stages[StageFileStep.RAW].extend(outputs)

        self.buildProcessingChain(processing, outputs, StageFileStep.PROCESSED)
    
    def addCombineStage(self, processing):
        self.buildProcessingChain(processing, self.stages[StageFileStep.PROCESSED], StageFileStep.COMBINED)
    
    def pushPreDwC(self, verbose=False):
        fileStages = (StageFileStep.RAW, StageFileStep.PROCESSED, StageFileStep.COMBINED, StageFileStep.PRE_DWC)
        
        for idx, stage in enumerate(fileStages[:-1], start=1):
            nextStage = fileStages[idx]
            if self.stages[stage] and not self.stages[nextStage]: # If this stage has files and next doesn't
                for stageFile in self.stages[stage].copy():
                    stageFile.updateStage(nextStage)
                    self.stages[nextStage].append(stageFile)
                    
                if verbose:
                    print(f"Pushed files {stage.name} --> {nextStage.name}")

        for file in self.stages[StageFileStep.PRE_DWC]:
            conversionScript = StageDWCConversion(file, self.dwcProcessor)
            dwcOutput = conversionScript.getOutput()
            convertedFile = StageFile(dwcOutput, {}, conversionScript, StageFileStep.DWC)
            self.stages[StageFileStep.DWC].append(convertedFile)
