from lib.settings import Settings
import logging
import toml
from typing import Any
from requests.auth import HTTPBasicAuth

class Secrets:
    def __init__(self, location: str = ""):
        self.email = ""
        self.id = ""
        self.username = ""
        self.password = ""
        self.key = ""

        self._location = location

        self._loadFromFile(location)

    def _loadFromFile(self, location: str) -> None:
        settings = Settings()

        with open(settings.Files.SECRETS) as fp:
            data = toml.load(fp)
        
        locationData = {}
        if location:
            if location not in data or not isinstance(data[location], dict):
                logging.error(f"No secrets found for: {location}")
                return
            
            locationData: dict = data.pop(location)

        data: dict = {key: value for key, value in data.items() if not isinstance(value, dict)} | locationData

        for key, value in data.items():
            setattr(self, key, value)

    def __getattribute__(self, name) -> Any:
        value = super().__getattribute__(name)

        if name in ("email", "id", "username", "password", "key") and not value:
            logging.warning(f"No secret property '{name}' found" + f" for location '{self._location}'" if self._location else "")

        return value

    def getAuth(self) -> HTTPBasicAuth:
        return HTTPBasicAuth(self.username, self.password)
