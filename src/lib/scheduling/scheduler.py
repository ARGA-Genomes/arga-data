import json
import time
from lib.scheduling.updaters import createUpdater, Updater

class Scheduler:
    def __init__(self):
        with open("sourceUpdates.json") as fp:
            sourceUpdates: dict[str, dict] = json.load(fp)

        self.sources: dict[str, Updater] = {}
        self.queue: list[Updater] = []

        for location, database in sourceUpdates.items():
            for name, properties in database.items():
                self.sources[f"{location}-{name}"] = createUpdater(location, name, properties)

    def _getSourceTimes(self) -> dict[str, int]:
        updateTimes = {source: updater.getTimeTilUpdate() for source, updater in self.sources.items()}
        return {k: v for k, v in sorted(updateTimes.items(), key=lambda x: x[1])}

    def run(self):
        while True:
            updateTimes = self._getSourceTimes()
            for source, timeTilUpdate in updateTimes.items():
                if timeTilUpdate == 0:
                    self.queue.append(source)
                    print(f"Updating {source}...")
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
            
if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.run()
