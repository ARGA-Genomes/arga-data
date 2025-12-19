from pathlib import Path
import json

class JsonSynchronizer:
    def __init__(self, filePath: Path):
        self._path = filePath
        self._data = {}

        self._load()

    def __setitem__(self, key: str, value: any) -> None:
        if key in self._data and self._data[key] == value:
            return
        
        self._data[key] = value
        self._sync()

    def __getitem__(self, key: str) -> any:
        return self._data[key]
    
    def __repr__(self) -> str:
        return str(self)
    
    def __str__(self) -> str:
        return str(self._data)

    def _load(self) -> None:
        if not self._path.exists():
            return
        
        with open(self._path) as fp:
            self._data = json.load(fp)

    def _sync(self) -> None:
        with open(self._path, "w") as fp:
            json.dump(self._data, fp, indent=4)

    def get(self, key: str, default: any = None) -> any:
        return self._data.get(key, default)

    def clear(self) -> None:
        if not self._data:
            return
        
        self._data = {}
        self._sync()
