import json
from pathlib import Path
from lib.processing.stages import Step
from lib.tools.logger import Logger
from datetime import datetime

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

    def updateMetadata(self, stepIndex: int, metadata: dict) -> None:
        taskMetadata: list[dict] = self.metadata.get(self.taskName, [])
        if stepIndex < len(taskMetadata):
            taskMetadata[stepIndex] = metadata
        else:
            taskMetadata.append(metadata)

        self.metadata[self.taskName] = taskMetadata
        self._syncMetadata()

        Logger.info(f"Updated {self.stepKey} metadata and saved to file")

    def updateTotalTime(self, totalTime: float) -> None:
        self.metadata["totalTime"] = totalTime
        self._syncMetadata()

    def getLastUpdate(self) -> datetime | None:
        if not self.metadata:
            return None
        
        return datetime.fromisoformat(self.metadata[self.taskName][0]["timestamp"])
