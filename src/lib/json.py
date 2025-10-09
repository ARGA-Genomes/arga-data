from pathlib import Path
import json

class JsonSynchronizer:
    def __init__(self, filePath: Path, loadOnInit: bool, asList: bool = False):
        self.path = filePath
        self.data = {} if not asList else []

        if loadOnInit:
            self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        
        with open(self.path) as fp:
            self.data = json.load(fp)

    def sync(self) -> None:
        with open(self.path, "w") as fp:
            json.dump(self.data, fp, indent=4)
