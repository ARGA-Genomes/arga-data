from pathlib import Path
from lib.processing.stages import File
from lib.processing.scripts import Script

class _Node:
    def __init__(self, file: File):
        self.file = file
        self.lowerLink = None

    def attachLink(self, link: '_Link'):
        self.lowerLink = link

    def runLink(self, overwrite: bool = False):
        if self.lowerLink is not None:
            self.lowerLink.execute(overwrite)

class _Link:
    def __init__(self, script: Script, childNode: _Node):
        self.script = script
        self.childNode = childNode
        self.executed = False

    def execute(self, overwrite: bool = False, chainExecute: bool = True):
        if self.executed:
            return
        
        self.script.run(overwrite)
        self.executed = True
        
        if chainExecute:
            self.childNode.runLink(overwrite)

class ProcessingChain:
    def __init__(self, rootNode: _Node):
        self.rootNode = rootNode
        self.lowestNode = rootNode

    def attachLink(self, link: _Link):
        self.lowestNode.attachLink(link)
        self.lowestNode = link.childNode

    def run(self, overwrite: bool, verbose: bool):
        self.rootNode.runLink(overwrite)

class ProcessingManager:
    def __init__(self, baseDir: Path, processingDir: Path):
        self.baseDir = baseDir
        self.processingDir = processingDir
        self.chains: list[ProcessingChain] = []

    def _createLink(self, step: dict, inputs: list[File]) -> _Link:
        script = Script(self.baseDir, self.processingDir, step, inputs)
        outputNode = _Node(script.output)
        return _Link(script, outputNode)

    def process(self, overwrite: bool = False, verbose: bool = False) -> None:
        if not self.processingDir.exists():
            self.processingDir.mkdir()

        for chain in self.chains:
            chain.run(overwrite, verbose)

    def getLatestNodes(self) -> list[File]:
        return [chain.lowestNode.file for chain in self.chains]

    def registerFile(self, file: File, processingSteps: list[dict]) -> ProcessingChain:
        node = _Node(file)
        chain = ProcessingChain(node)

        self.addProcessing(chain, processingSteps)
        self.chains.append(chain)

    def addProcessing(self, chain: ProcessingChain, processingSteps: list[dict]):
        for step in processingSteps:
            link = self._createLink(step, [chain.lowestNode.file])
            chain.attachLink(link)

    def addAllProcessing(self, processingSteps: list[dict]):
        if not processingSteps:
            return
        
        for chain in self.chains:
            self.addProcessing(chain, processingSteps)

    def addFinalProcessing(self, processingSteps: list[dict]):
        if not processingSteps:
            return
        
        # First step of final processing should combine all chains to a single file
        inputs = [node.file for node in [chain.lowestNode for chain in self.chains]]
        link = self._createLink(processingSteps[0], inputs)

        for chain in self.chains:
            chain.attachLink(link)

        # All chains now point to the same node, add remaining processing steps to first chain
        self.addProcessing(self.chains[0], processingSteps[1:])
