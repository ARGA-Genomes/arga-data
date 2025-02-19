import json
from pathlib import Path
from lib.tools.logger import Logger
from datetime import datetime
import time
from enum import Enum

class Metadata(Enum):
    OUTPUT = "output"
    SUCCESS = "success"
    DURATION = "duration"
    TIMESTAMP = "timestamp"
    TOTAL_TIME = "totalTime"
    CUSTOM = ""

class Task:
    def getOutputPath(self) -> Path:
        raise NotImplementedError

    def runTask(self) -> bool:
        raise NotImplementedError

class SystemManager:
    def __init__(self, baseDir: Path, stepKey: str, taskName: str):
        self.baseDir = baseDir
        self.stepKey = stepKey
        self.taskName = taskName
        self.metadataPath = baseDir / "metadata.json"
        self._loadMetadata()

    def _getFileMetadata(self) -> dict:
        if not self.metadataPath.exists():
            return {}
        
        with open(self.metadataPath) as fp:
            try:
                return json.load(fp)
            except json.JSONDecodeError:
                return {}
        
    def _loadMetadata(self) -> None:
        self.metadata = self._getFileMetadata().get(self.stepKey, {})

    def _syncMetadata(self) -> None:
        data = self._getFileMetadata()
        data[self.stepKey] = self.metadata

        with open(self.metadataPath, "w") as fp:
            json.dump(data, fp, indent=4)

    def runTasks(self, tasks: list[Task], *args) -> bool:
        allSucceeded = False
        startTime = time.perf_counter()
        for idx, task in enumerate(tasks):
            taskStart = time.perf_counter()
            success = task.runTask(*args)

            self.updateMetadata(idx, {
                Metadata.OUTPUT: task.getOutputPath().name,
                Metadata.SUCCESS: success,
                Metadata.DURATION: time.perf_counter() - taskStart,
                Metadata.TIMESTAMP: datetime.now().isoformat()
            })

            allSucceeded = allSucceeded and success

        self.updateTotalTime(time.perf_counter() - startTime)
        return allSucceeded

    def updateMetadata(self, stepIndex: int, metadata: dict[Metadata, any]) -> None:
        parsedMetadata = {}
        for key, value in metadata.items():
            if not isinstance(key, Metadata):
                continue

            if key == Metadata.CUSTOM:
                for customKey, customValue in value.items():
                    parsedMetadata[customKey] = customValue

                continue

            parsedMetadata[key.value] = value

        taskMetadata: list[dict] = self.metadata.get(self.taskName, [])
        if stepIndex < len(taskMetadata):
            taskMetadata[stepIndex] = parsedMetadata
        else:
            taskMetadata.append(parsedMetadata)

        self.metadata[self.taskName] = taskMetadata
        self._syncMetadata()

        Logger.info(f"Updated {self.stepKey} metadata and saved to file")

    def updateTotalTime(self, totalTime: float) -> None:
        self.metadata[Metadata.TOTAL_TIME.value] = totalTime
        self._syncMetadata()

    def getLastUpdate(self) -> datetime | None:
        if not self.metadata:
            return None
        
        return datetime.fromisoformat(self.metadata[self.taskName][0][Metadata.TIMESTAMP.value])
