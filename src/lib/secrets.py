from lib.settings import Settings
import logging
from typing import Any
import toml

class Property(Enum):
    EMAIL = "email"
    ID = "id"
    USERNAME = "username"
    PASSWORD = "password"
    API_KEY = "api"

class Secrets:
    def __init__(self):
        self._path = None

    def get(self, property: Property, location: str = "") -> Any:
        if not self._path:
            self._resolvePath()

        data = self._load()

        if not location:
            return data.get(property.value, None)
        
        subsection = data.get(location, None)
        if subsection is None:
            logging.warning(f"No location found '{location}'")
            return None
        
        if property.value not in subsection:
            logging.warning(f"Location '{location}' has no property '{property.value}'")
            return None
        
        return subsection[property.value]

    def _resovlePath(self):
        settings = Settings()
        self._path = settings.Files.SECRETS

    def _load(self):
        with open(self._path) as fp:
            return toml.load(fp)
