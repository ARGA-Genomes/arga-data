import toml
import logging
from pathlib import Path
from typing import Any

class _AttrDict:
    def __init__(self, data: dict[str, any], _depth: int = 0):
        self._data = {}
        self._depth = _depth

        self.update(data)

    def __str__(self) -> str:
        return str(self._data)

    def __repr__(self) -> Any:
        return str(self)

    def __getattr__(self, name: str) -> Any | None:
        value = self._data.get(name, None)
        if value is None:
            logging.warning(f"No attribute found: {name}")

        return value
    
    def copy(self) -> '_AttrDict':
        return _AttrDict(dict(self._data), self._depth)
    
    def update(self, data: dict[str, any]) -> None:
        for key, value in data.items():
            if isinstance(value, dict):
                value = _AttrDict(value)

            self._data[key] = value

class TomlLoader(_AttrDict):
    def __init__(self, path: Path):
        super().__init__(self._loadToml(path))
    
    def _loadToml(self, path: Path) -> dict:
        if not path.exists():
            raise AttributeError from Exception(f"No toml file found at specified path")

        with open(path) as fp:
            return toml.load(fp)

    def createChild(self, childPath: Path) -> 'TomlLoader':
        childData = self._loadToml(childPath)

        newChild = self.copy()
        newChild.update(childData)
        return newChild
