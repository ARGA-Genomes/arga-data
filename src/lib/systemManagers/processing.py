from pathlib import Path
from lib.processing.stages import File
from lib.processing.scripts import FileScript, FileSelect
from lib.systemManagers.baseManager import SystemManager, Task
from lib.tools.logger import Logger

class _Node(Task):
    def __init__(self, index: int, script: FileScript, parents: list['_Node']):
        self.index = index
        self.script = script
        self.parents = parents

    def __eq__(self, other: '_Node'):
        return self.index == other.index

    def getOutputPath(self) -> Path:
        return self.script.output.filePath

    def getOutputFile(self) -> File:
        return self.script.output
    
    def getRequirements(self, tasks: list['_Node']) -> list['_Node']:
        newTasks = []

        for parent in self.parents:
            if parent not in tasks:
                newTasks.extend(parent.getRequirements(tasks + newTasks))

        newTasks.append(self)
        return newTasks

    def runTask(self, overwrite: bool, verbose: bool) -> bool:
        return self.script.run(overwrite, verbose)

class _Root(_Node):
    def __init__(self, index: int, file: File):
        self.index = index
        self.file = file

    def getOutputPath(self) -> Path:
        return self.file.filePath

    def getOutputFile(self) -> File:
        return self.file
    
    def getRequirements(self, *args) -> list[_Node]:
        return []
    
    def runTask(self, *args) -> bool:
        return True

class ProcessingManager(SystemManager):
    def __init__(self, dataDir: Path):
        self.stepName = "processing"

        super().__init__(dataDir.parent, self.stepName, "steps")

        self.processingDir = dataDir / self.stepName
        self.nodes: dict[FileSelect, list[_Node]] = {
            FileSelect.DOWNLOAD: [],
            FileSelect.PROCESS: []
        }

    def _createNode(self, step: dict, parents: list[_Node]) -> _Node | None:
        inputs = {FileSelect.INPUT: self.getLatestNodeFile()} | {select: [node.getOutputFile() for node in nodes] for select, nodes in self.nodes.items()}
        
        try:
            script = FileScript(self.baseDir, dict(step), self.processingDir, inputs)
        except AttributeError as e:
            Logger.error(f"Invalid processing script configuration: {e}")
            return None
        
        return _Node(len(self.nodes[FileSelect.PROCESS]), script, parents)
    
    def _addProcessing(self, node: _Node, processingSteps: list[dict]) -> None:
        for step in processingSteps:
            subNode = self._createNode(step, [node])
            self.nodes[FileSelect.PROCESS].append(subNode)
            node = subNode
        return node
    
    def getLatestNodeFile(self) -> File:
        latestNode = self.nodes[FileSelect.DOWNLOAD][-1] if not self.nodes[FileSelect.PROCESS] else self.nodes[FileSelect.PROCESS][-1]
        return latestNode.getOutputFile()

    def process(self, overwrite: bool = False, verbose: bool = False) -> bool:
        if all(isinstance(node, _Root) for node in self.nodes): # All root nodes, no processing required
            Logger.info("No processing required for any nodes")
            return True

        if not self.processingDir.exists():
            self.processingDir.mkdir()

        queuedTasks: list[_Node] = []
        for node in self.nodes:
            requirements = node.getRequirements(queuedTasks)
            queuedTasks.extend(requirements)

        return self.runTasks(queuedTasks, overwrite, verbose)

    def registerFile(self, file: File, processingSteps: list[dict]) -> bool:
        node = _Root(len(self.nodes[FileSelect.DOWNLOAD]), file)
        self.nodes[FileSelect.DOWNLOAD].append(node)
        self._addProcessing(node, list(processingSteps))

    def addSpecificProcessing(self, processingSteps: list[dict]) -> bool:
        if not processingSteps:
            return
        
        for idx, node in enumerate(self.nodes):
            self.nodes[idx] = self._addProcessing(node, processingSteps)

    def addFinalProcessing(self, processingSteps: list[dict]) -> bool:
        if not processingSteps:
            return
        
        # First step of final processing should combine all chains to a single file
        finalNode = self._createNode(processingSteps[0], self.nodes)
        self.nodes = [self._addProcessing(finalNode, processingSteps[1:])]
