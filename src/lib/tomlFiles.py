import toml
import logging
from pathlib import Path
from typing import Any

class _AttrDict:
    def __init__(self, data: dict[str, any]):
        self._data = data

    def __str__(self) -> str:
        return str(self._data)

    def __repr__(self) -> Any:
        return str(self)

    def __getattr__(self, name: str) -> Any | None:
        value = self._data.get(name, None)
        if value is None:
            logging.warning(f"No attribute found: {name}")

        return value

class TomlLoader(_AttrDict):
    def __init__(self, path: Path, _inheritedData: dict = {}, _skipLoad: bool = False):
        self._path = path
        data = {} if _skipLoad else self._loadToml(path)

        super().__init__(self._parseData(_inheritedData | data))

    def _loadToml(self, path: Path) -> dict:
        if not isinstance(path, Path):
            raise Exception(f"'{path}' is not a valid path") from AttributeError

        if not path.exists():
            raise Exception(f"No file found at path: {path}") from AttributeError

        with open(path) as fp:
            return toml.load(fp)
    
    def _parseData(self, data: dict) -> dict:
        res = {}
        for key, value in data.items():
            value = self.parse(value)

            if isinstance(value, dict):
                value = self.__class__(self._path, value, True)

            res[key] = value

        return res
    
    def parse(self, value: any) -> Any:
        return value

    def createChild(self, childPath: Path) -> 'TomlLoader':
        return self.__class__(childPath, self._data)
