import json
from datetime import datetime
from pathlib import Path

class TimeManager:
    def __init__(self, databaseDir: Path):
        self.timeFile = databaseDir / f"refresh.json"

        if not self.timeFile.exists():
            self.timeData = []
            return

        with open(self.timeFile) as fp:
            self.timeData = json.load(fp)

    def update(self, records: int):
        self.timeData.append((datetime.now().isoformat(), records))
        
        with open(self.timeFile, "w") as fp:
            json.dump(self.timeData, fp, indent=4)

    def getLastUpdate(self) -> tuple[datetime, int]:
        if not self.timeData:
            return None, None
        
        lastRefresh = self.timeData[-1]
        return datetime.fromisoformat(lastRefresh[0]), lastRefresh[1]