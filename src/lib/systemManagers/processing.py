from pathlib import Path
from lib.processing.files import DataFile, Step
from lib.processing.scripts import FileScript, FileSelect
from lib.systemManagers.baseManager import SystemManager, Task
import logging

class _Node(Task):
    def __init__(self, index: str, script: FileScript, parents: list['_Node']):
        super().__init__()

        self.index = index
        self.script = script
        self.parents = parents

    def __eq__(self, other: '_Node'):
        return self.index == other.index

    def getOutputPath(self) -> Path:
        return self.script.output.path

    def getOutputFile(self) -> DataFile:
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
    def __init__(self, index: int, file: DataFile):
        self.index = index
        self.file = file

    def getOutputPath(self) -> Path:
        return self.file.path

    def getOutputFile(self) -> DataFile:
        return self.file
    
    def getRequirements(self, *args) -> list[_Node]:
        return []
    
    def runTask(self, *args) -> bool:
        return True

class ProcessingManager(SystemManager):
    def __init__(self, dataDir: Path, scriptDir: Path, metadataDir: Path, scriptImports: dict[str, Path]):
        super().__init__(dataDir, scriptDir, metadataDir, Step.PROCESSING, "steps")

        self.scriptImports = scriptImports

        self._rootNodes: list[_Node] = []
        self._scriptNodes: list[list[_Node]] = []

    def _createNode(self, step: dict, parents: list[_Node], depth: int) -> _Node | None:
        fileInput = self._rootNodes[-1] if depth == 0 else self._scriptNodes[depth-1][-1]
        flatScriptNodes = [node for nodeList in self._scriptNodes for node in nodeList]

        inputs = {
            FileSelect.INPUT: [fileInput.getOutputFile()],
            FileSelect.DOWNLOAD: [node.getOutputFile() for node in self._rootNodes],
            FileSelect.PROCESS: [node.getOutputFile() for node in flatScriptNodes]
        }
        
        try:
            script = FileScript(self.scriptDir, dict(step), self.workingDir, inputs, self.scriptImports)
        except AttributeError as e:
            logging.error(f"Invalid processing script configuration: {e}")
            return None
        
        node = _Node(f"P{len(flatScriptNodes)}", script, parents)
        if depth >= len(self._scriptNodes):
            self._scriptNodes.append([])

        self._scriptNodes[-1].append(node)
        return node
    
    def _addProcessing(self, node: _Node, processingSteps: list[dict], startingDepth: int) -> None:
        nextNode = node
        for idx, step in enumerate(processingSteps):
            subNode = self._createNode(step, [nextNode], startingDepth + idx)
            nextNode = subNode
    
    def getLatestNodeFile(self) -> DataFile:
        latestNode = self._rootNodes[-1] if not self._scriptNodes else self._scriptNodes[-1][-1]
        return latestNode.getOutputFile()

    def process(self, overwrite: bool = False, verbose: bool = False) -> bool:
        if not self._scriptNodes: # All root nodes, no processing required
            logging.info("No processing required for any nodes")
            return True

        for node in self._scriptNodes[-1]:
            requirements = node.getRequirements(self._tasks)
            self._tasks.extend(requirements)

        return self.runTasks(overwrite, verbose)

    def registerFile(self, file: DataFile, processingSteps: list[dict]) -> None:
        node = _Root(f"D{len(self._rootNodes)}", file)
        self._rootNodes.append(node)
        self._addProcessing(node, list(processingSteps), 0)

    def addFinalProcessing(self, processingSteps: list[dict]) -> None:
        finalNode = self._createNode(processingSteps[0], self._scriptNodes[-1], len(self._scriptNodes))
        self._addProcessing(finalNode, processingSteps[1:], len(self._scriptNodes))
