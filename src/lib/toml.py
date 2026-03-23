import toml
from pathlib import Path
from typing import Any
from enum import Enum

class TomlLoader:
    def __init__(self, path: Path, loadOnInit: bool = False):
        self._path = path
        self._data = {}

        if loadOnInit:
            self.load()

    def load(self) -> None:
        if self._data():
            return
        
        if not isinstance(self._path, Path):
            raise Exception(f"'{self._path}' is not a valid path") from AttributeError

        if not self._path.exists():
            raise Exception(f"No file found at path: {self._path}") from AttributeError

        with open(self._path) as fp:
            return toml.load(fp)
    
    def get(self, field: Enum) -> Any:
        ...
