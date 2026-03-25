from lib.settings import Settings
import logging
from typing import Any
import toml
from enum import Enum

class SecretProperty(Enum):
    EMAIL = "email"
    ID = "id"
    USERNAME = "username"
    PASSWORD = "password"
    API_KEY = "key"

class Secrets:
    def __init__(self):
        self._path = None

    def get(self, property: SecretProperty, location: str = "", defaultValue: Any = None) -> Any:
        if not self._path:
            self._resovlePath()

        data = self._load()

        if not location:
            return data.get(property.value, None)
        
        subsection = data.get(location, None)
        if subsection is None:
            return defaultValue
        
        if property.value not in subsection:
            return defaultValue
        
        return subsection[property.value]

    def _resovlePath(self):
        settings = Settings()
        self._path = settings.Files.SECRETS

    def _load(self):
        with open(self._path) as fp:
            return toml.load(fp)
