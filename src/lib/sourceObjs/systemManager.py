from pathlib import Path
from lib.processing.stageFile import StageFileStep, StageFile
from lib.processing.stageScript import StageDownloadScript, StageScript, StageDWCConversion
from lib.processing.parser import SelectorParser
from lib.processing.dwcProcessor import DWCProcessor

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

        self.downloadDir = self.rootDir / "raw"
        self.processingDir = self.rootDir / "processing"
        self.preConversionDir = self.rootDir / "preConversion"
        self.dwcDir = self.rootDir / "dwc"
        
        self.parser = SelectorParser(self.rootDir, self.downloadDir, self.processingDir, self.preConversionDir, self.dwcDir)
        self.dwcProcessor = DWCProcessor(self.location, self.dwcProperties, self.enrichDBs, self.dwcDir)

        self.stages = {stage: [] for stage in StageFileStep}

    def getFiles(self, stage: StageFileStep):
        return self.stages[stage]
    
    def create(self, stage: StageFileStep, fileNumbers: list[int] = [], overwrite: int = 0):
        if not fileNumbers: # Create all files:
            for file in self.stages[stage]:
                file.create(stage, overwrite)
            return
        
        for number in fileNumbers:
            if number >= 0 and number <= len(self.stages[stage]):
                self.stages[stage][number].create(stage, overwrite)
            else:
                print(f"Invalid number provided: {number}")

    def buildProcessingChain(self, processingSteps: list[dict], initialInputs: list[StageFile], finalStage: StageFileStep) -> None:
        inputs = initialInputs.copy()
        for idx, step in enumerate(processingSteps):
            scriptStep = StageScript(step, inputs, self.parser)
            stage = StageFileStep.INTERMEDIATE if idx < len(processingSteps) else finalStage
            inputs = [StageFile(filePath, {}, scriptStep, stage) for filePath in scriptStep.getOutputs()]

        self.stages[finalStage].extend(inputs)

    def addDownloadURLStage(self, url: str, fileName: str, processing: list[dict], fileProperties: dict = {}):
        downloadedFile = self.downloadDir / fileName # downloaded files go into download directory
        downloadScript = StageDownloadScript(url, downloadedFile, self.parser, self.user, self.password)
        
        rawFile = StageFile(downloadedFile, {} if processing else fileProperties, downloadScript, StageFileStep.RAW)
        self.stages[StageFileStep.RAW].append(rawFile)

        self.buildProcessingChain(processing, [rawFile], StageFileStep.PROCESSED)

    def addRetrieveScriptStage(self, script, processing, fileProperties):
        scriptStep = StageScript(script, [], self.parser)
        outputs = [StageFile(filePath, fileProperties, scriptStep, StageFileStep.RAW) for filePath in scriptStep.getOutputs()]
        self.stages[StageFileStep.RAW].extend(outputs)

        self.buildProcessingChain(processing, outputs, StageFileStep.PROCESSED)
    
    def addCombineStage(self, processing):
        self.buildProcessingChain(processing, self.stages[StageFileStep.PROCESSED], StageFileStep.COMBINED)
    
    def pushPreDwC(self):
        fileStages = (StageFileStep.RAW, StageFileStep.PROCESSED, StageFileStep.COMBINED, StageFileStep.PRE_DWC)
        for idx, stage in enumerate(fileStages[:-1], start=1):
            nextStage = fileStages[idx]
            if self.stages[stage] and not self.stages[nextStage]: # If this stage has files and next doesn't
                self.stages[nextStage] = self.stages[stage].copy()

        for file in self.stages[StageFileStep.PRE_DWC]:
            conversionScript = StageDWCConversion(file, self.dwcProcessor)
            dwcOutput = conversionScript.getOutput()
            convertedFile = StageFile(dwcOutput, {}, conversionScript, StageFileStep.DWC)
            self.stages[StageFileStep.DWC].append(convertedFile)
