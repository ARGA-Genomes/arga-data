from pathlib import Path
from lib.processing.stages import File
from lib.processing.scripts import FileScript, FileSelect
from lib.systemManagers.baseManager import SystemManager, Task
import logging

class _Node(Task):
    def __init__(self, index: str, script: FileScript, parents: list['_Node']):
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
        return self.script.run(overwrite, verbose)[0] # No retval for processing tasks, just return success

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
    def __init__(self, baseDir: Path, dataDir: Path, importDir: Path):
        self.stepName = "processing"
        self.importDir = importDir

        super().__init__(baseDir, self.stepName, "steps")

        self.processingDir = dataDir / self.stepName
        self.nodes: dict[FileSelect, list[_Node]] = {
            FileSelect.DOWNLOAD: [],
            FileSelect.PROCESS: []
        }
        
        self._lowestNodes: list[_Node] = []

    def _createNode(self, step: dict, parents: list[_Node]) -> _Node | None:
        inputs = {FileSelect.INPUT: [self.getLatestNodeFile()]} | {select: [node.getOutputFile() for node in nodes] for select, nodes in self.nodes.items()}
        
        try:
            script = FileScript(self.baseDir, dict(step), self.processingDir, inputs, [str(self.importDir)])
        except AttributeError as e:
            logging.error(f"Invalid processing script configuration: {e}")
            return None
        
        node = _Node(f"P{len(self.nodes[FileSelect.PROCESS])}", script, parents)
        self.nodes[FileSelect.PROCESS].append(node)
        return node
    
    def _addProcessing(self, node: _Node, processingSteps: list[dict]) -> _Node:
        for step in processingSteps:
            subNode = self._createNode(step, [node])
            node = subNode
        return node
    
    def getLatestNodeFile(self) -> File:
        latestNode = self.nodes[FileSelect.DOWNLOAD][-1] if not self.nodes[FileSelect.PROCESS] else self.nodes[FileSelect.PROCESS][-1]
        return latestNode.getOutputFile()

    def process(self, overwrite: bool = False, verbose: bool = False) -> bool:
        if all(isinstance(node, _Root) for node in self._lowestNodes): # All root nodes, no processing required
            logging.info("No processing required for any nodes")
            return True

        if not self.processingDir.exists():
            self.processingDir.mkdir()

        queuedTasks: list[_Node] = []
        for node in self._lowestNodes:
            requirements = node.getRequirements(queuedTasks)
            queuedTasks.extend(requirements)

        return self.runTasks(queuedTasks, overwrite, verbose)

    def registerFile(self, file: File, processingSteps: list[dict]) -> None:
        node = _Root(f"D{len(self.nodes[FileSelect.DOWNLOAD])}", file)
        self.nodes[FileSelect.DOWNLOAD].append(node)
        lowestNode = self._addProcessing(node, list(processingSteps))
        self._lowestNodes.append(lowestNode)

    def addFinalProcessing(self, processingSteps: list[dict]) -> None:
        if not processingSteps:
            return

        finalNode = self._createNode(processingSteps[0], self._lowestNodes)
        self._lowestNodes = [self._addProcessing(finalNode, processingSteps[1:])]
