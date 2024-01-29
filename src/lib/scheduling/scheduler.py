import json
from lib.scheduling.updaters import createUpdater

class Scheduler:
    def __init__(self):
        with open("sourceUpdates.json") as fp:
            sourceUpdates: dict[str, dict] = json.load(fp)

        self.sources = []
        self.queue = []

        for location, database in sourceUpdates.items():
            for name, properties in database.items():
                self.sources.append(createUpdater(location, name, properties))

    def run(self):
        for source in self.sources:
            print(source._calcInterval())

if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.run()

