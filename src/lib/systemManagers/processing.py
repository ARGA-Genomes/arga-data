from pathlib import Path
from lib.processing.stages import File
from lib.processing.scripts import Script
from lib.systemManagers.baseManager import SystemManager
from lib.tools.logger import Logger
import time
from datetime import datetime

class _Node:
    def __init__(self, index: int, script: Script, parents: list['_Node']):
        self.index = index
        self.script = script
        self.parents = parents

    def __eq__(self, other: '_Node'):
        return self.index == other.index

    def getOutput(self) -> File:
        return self.script.output
    
    def getFunction(self) -> str:
        return self.script.function
    
    def getRequirements(self, tasks: list['_Node']) -> list['_Node']:
        newTasks = []

        for parent in self.parents:
            if parent not in tasks:
                newTasks.extend(parent.getRequirements(tasks + newTasks))

        newTasks.append(self)
        return newTasks

    def execute(self, overwrite: bool, verbose: bool) -> bool:
        return self.script.run(overwrite, verbose)

class _Root(_Node):
    def __init__(self, file: File):
        self.file = file

    def getOutput(self) -> File:
        return self.file
    
    def getRequirements(self, *args) -> list[_Node]:
        return []
    
    def execute(self, *args) -> bool:
        return True

class ProcessingManager(SystemManager):
    def __init__(self, baseDir: Path, processingDir: Path):
        super().__init__(baseDir, "processing", "steps")

        self.processingDir = processingDir
        self.nodes: list[_Node] = []
        self._nodeIndex = 0

    def _createNode(self, step: dict, parents: list[_Node]) -> _Node | None:
        inputs = [node.getOutput() for node in parents]
        try:
            script = Script(self.baseDir, self.processingDir, dict(step), inputs)
        except AttributeError as e:
            Logger.error(f"Invalid processing script configuration: {e}")
            return None
        
        node = _Node(self._nodeIndex, script, parents)
        self._nodeIndex += 1
        return node
    
    def _addProcessing(self, node: _Node, processingSteps: list[dict]) -> _Node:
        for step in processingSteps:
            subNode = self._createNode(step, [node])
            node = subNode
        return node
    
    def getLatestNodeFiles(self) -> list[File]:
        return [node.getOutput() for node in self.nodes]

    def process(self, overwrite: bool = False, verbose: bool = False) -> bool:
        if all(isinstance(node, _Root) for node in self.nodes): # All root nodes, no processing required
            Logger.info("No processing required for any nodes")
            return True, {}

        if not self.processingDir.exists():
            self.processingDir.mkdir()

        queuedTasks: list[_Node] = []
        for node in self.nodes:
            requirements = node.getRequirements(queuedTasks)
            queuedTasks.extend(requirements)

        allSucceeded = True
        startTime = time.perf_counter()
        for idx, task in enumerate(queuedTasks):
            stattTime = time.perf_counter()
            success = task.execute(overwrite, verbose)

            self.updateMetadata(idx, {
                "function": task.getFunction(),
                "output": task.getOutput().filePath.name,
                "success": success,
                "duration": time.perf_counter() - stattTime,
                "timestamp": datetime.now().isoformat()
            })

            allSucceeded = allSucceeded and success

        self.updateTotalTime(time.perf_counter() - startTime)

        return allSucceeded

    def registerFile(self, file: File, processingSteps: list[dict]) -> bool:
        node = _Root(file)
        node = self._addProcessing(node, processingSteps)
        self.nodes.append(node)

    def addAllProcessing(self, processingSteps: list[dict]) -> bool:
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
