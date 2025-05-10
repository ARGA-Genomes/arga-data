from pathlib import Path
import toml
from typing import Any
import logging

class _ConfigCollection:
    def __init__(self, configs: dict[str, any], relativePath: Path):
        self._configs = configs

        for configName, value in self._configs.items():
            if isinstance(value, str):
                if value.startswith("./"):
                    value = relativePath / value[2:]

                elif value.startswith("/") or value[1:].startswith(":/"):
                    value = Path(value)

            self._configs[configName] = value

    def __getattr__(self, name: str) -> Any | None:
        value = self._configs.get(name, None)
        if value is None:
            logging.warning(f"No config setting found: {name}")

        return value
    
    def __str__(self) -> str:
        return str({configName: configValue for configName, configValue in self._configs.items()})
    
    def _inherit(self, collection: '_ConfigCollection'):
        for configName, configValue in collection._configs.items():
            if configName not in self._configs:
                self._configs[configName] = configValue

class Configs:
    def __init__(self, path: Path):
        self._path = path
        if not path.exists():
            raise AttributeError from Exception(f"No config file found at specified path")

        with open(path) as fp:
            self._collections = toml.load(fp)

        for collectionName, configs in self._collections.items():
            self._collections[collectionName] = _ConfigCollection(configs, path.parent)

    def __getattr__(self, name: str) -> Any | None:
        value = self._collections.get(name, None)
        if value is None:
            logging.warning(f"No config category found: {name}")

        return value
    
    def __str__(self) -> str:
        return str({collectionName: str(collection) for collectionName, collection in self._collections.items()})

    def createChildConfigs(self, path: Path) -> 'Configs':
        try:
            childConfigs = Configs(path)
        except AttributeError:
            logging.warning(f"No config file found at {path}")
            return self

        for collectionName, collection in self._collections.items():
            childConfigs._inheritCollection(collectionName, collection)

        childConfigs._collections = {collectionName: childConfigs._collections[collectionName] for collectionName in self._collections} # Sort child the same way self is
        return childConfigs

    def _inheritCollection(self, collectionName: str, collection: _ConfigCollection) -> None:
        if collectionName not in self._collections:
            self._collections[collectionName] = collection
            return
        
        self._collections[collectionName]._inherit(collection)

globalConfig = Configs(Path(__file__).parents[2] / "config.toml")
