import json
from pathlib import Path
import logging
from datetime import datetime
import time
from enum import Enum
from typing import Any
from lib.data.database import Step
from lib.processing.tasks import Task

class Metadata(Enum):
    OUTPUT = "output"
    SUCCESS = "success"
    TASK_START = "task started"
    TASK_END = "task completed"
    TASK_DURATION = "duration"
    LAST_SUCCESS_START = "last success started"
    LAST_SUCCESS_END = "last success completed"
    LAST_SUCCESS_DURATION = "last success duration"
    TOTAL_DURATION = "total duration"
    LAST_SUCCESS_TOTAL_DURATION = "last success total duration"
    CUSTOM = ""

class MetadataManager:

    _metadataFileName = "metadata.json"

    def __init__(self, metadataDir: Path):
        self.metadataPath = metadataDir / self._metadataFileName
        self._loadMetadata()

    def _loadMetadata(self) -> None:
        if not self.metadataPath.exists():
            return self.metadata = {}
        
        with open(self.metadataPath) as fp:
            try:
                self.metadata = json.load(fp)
            except json.JSONDecodeError:
                self.metadata = {}

    def _syncMetadata(self) -> None:
        with open(self.metadataPath, "w") as fp:
            json.dump(self.metadata, fp, indent=4)

    def runTasks(self, step: Step, tasks: list[Task], overwrite: bool, verbose: bool) -> bool:
        allSucceeded = False
        startTime = time.perf_counter()
        for idx, task in enumerate(tasks):
            taskStartTime = time.perf_counter()
            taskStartDate = datetime.now().isoformat()

            success = task.runTask(overwrite, verbose)

            duration = time.perf_counter() - taskStartTime
            taskEndDate = datetime.now().isoformat()

            packet = {
                Metadata.OUTPUT: task.getOutputPath().name,
                Metadata.SUCCESS: success,
                Metadata.TASK_START: taskStartDate,
                Metadata.TASK_END: taskEndDate,
                Metadata.TASK_DURATION: duration,
            }

            # Task can set metadata on itself during run
            extraMetadata = task.getRunMetadata()
            if extraMetadata:
                packet[Metadata.CUSTOM] = extraMetadata

            if success:
                packet |= {
                    Metadata.LAST_SUCCESS_START: taskStartDate,
                    Metadata.LAST_SUCCESS_END: taskEndDate,
                    Metadata.LAST_SUCCESS_DURATION: duration
                }

            self.updateMetadata(step, idx, packet)
            allSucceeded = allSucceeded and success

        self.updateTotalTime(time.perf_counter() - startTime, allSucceeded)
        return allSucceeded

    def updateMetadata(self, step: Step, stepIndex: int, metadata: dict[Metadata, any]) -> None:
        parsedMetadata = {}
        for key, value in metadata.items():
            if not isinstance(key, Metadata):
                continue

            if key == Metadata.CUSTOM:
                for customKey, customValue in value.items():
                    parsedMetadata[customKey] = customValue

                continue

            parsedMetadata[key.value] = value

        taskMetadata: list[dict] = self.metadata.get(step.value, [])
        if stepIndex < len(taskMetadata):
            taskMetadata[stepIndex] |= parsedMetadata
        else:
            taskMetadata.append(parsedMetadata)

        self.metadata[step.value] = taskMetadata
        self._syncMetadata()

        logging.info(f"Updated {step.value} metadata and saved to file")

    def updateTotalTime(self, totalTime: float, allSucceeded) -> None:
        self.metadata[Metadata.TOTAL_DURATION.value] = totalTime
        if allSucceeded:
            self.metadata[Metadata.LAST_SUCCESS_TOTAL_DURATION.value] = totalTime

        self._syncMetadata()

    def getMetadata(self, step: Step, stepIndex: int, property: Metadata) -> None | Any:
        return self.metadata[step.value][stepIndex][property.value]

    def getLastUpdate(self, step: Step) -> datetime | None:
        timestamp = self.getMetadata(step.value, 0, Metadata.LAST_SUCCESS_START)
        if timestamp is not None:
            return datetime.fromisoformat(timestamp)

    def getLastOutput(self, step: Step) -> str | None:
        return self.getMetadata(step.value, -1, Metadata.OUTPUT)
