import json
import time
from lib.scheduling.updaters import createUpdater, Updater
from lib.processing.stageFile import StageFileStep
from lib.sourceManager import SourceManager

class Scheduler:
    def __init__(self):
        with open("sourceUpdates.json") as fp:
            sourceUpdates: dict[str, dict] = json.load(fp)

        self.sources: dict[str, Updater] = {}
        self.queue: list[Updater] = []
        self.manager = SourceManager()

        for location, database in sourceUpdates.items():
            for name, properties in database.items():
                self.sources[f"{location}-{name}"] = createUpdater(location, name, properties)

    def _getSourceTimes(self) -> dict[str, int]:
        updateTimes = {source: updater.getTimeTilUpdate() for source, updater in self.sources.items()}
        return {k: v for k, v in sorted(updateTimes.items(), key=lambda x: x[1])}
    
    def _update(self, source: str, extraKwargs: dict[StageFileStep, dict[str, str]] = {}) -> None:
        database = self.manager.getDB(source)
        database.prepareStage(StageFileStep.DWC)
        
        for stage in (StageFileStep.DOWNLOADED, StageFileStep.PRE_DWC, StageFileStep.DWC):
            print(f"Creating stage: {stage.name}")
            database.createStage(stage, overwrite=10, **extraKwargs.get(stage, {}))

    def run(self):
        updateTimes = self._getSourceTimes()

        while True:    
            for source, timeTilUpdate in updateTimes.items():
                if timeTilUpdate == 0:
                    print(f"Updating {source}...")
                    self._update(source)
                    continue

                sleepTime = timeTilUpdate
                break
            else:
                sleepTime = 0

            print(f"All sources updated, sleeping for {sleepTime}s until next source update")

            try:
                time.sleep(sleepTime)
            except KeyboardInterrupt:
                print("Exiting...")
                break

            updateTimes = self._getSourceTimes()
            
if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.run()
