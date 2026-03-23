from lib.toml import TomlLoader
from pathlib import Path
from enum import Enum
from typing import Any

rootDir = Path(__file__).parents[2]
dataSourcesDir = rootDir / "dataSources"
srcDir = rootDir / "src"
libDir = srcDir / "lib"
scriptsDir = srcDir / "scripts"
settingsPath = rootDir / "settings.toml"

class Settings(Enum):

    class _Folders(Enum):
        LOGS = "./logs"

    class _Storage(Enum):
        DATA = ""
        PACKAGE = ""

    class _Files(Enum):
        SECRETS = "./secrets.toml"

    class _Logging(Enum):
        LOG_TO_CONSOLE = True
        LOG_LEVEL = "info" # Log levels: debug, info, warning, error, critical

    FOLDERS = _Folders
    STORAGE = _Storage
    FILES = _Files
    LOGGING = _Logging

# class SettingsFile(TomlLoader):
#     def parse(self, value: any) -> Any:
#         if isinstance(value, str):
#             if value.startswith("./"):
#                 return self._path.parent / value[2:]

#             if value.startswith("/") or value[1:].startswith(":/"):
#                 return Path(value)
        
#         return value

# def generate() -> None:
#     _defaultSettings = """
#         [folders]
#         logs = "./logs" # Location of all logging files, cannot overwrite with local config files

#         [storage]
#         data = "" # Location overwrite for source data including downloading/processing/conversion, leave blank to keep in respective dataSource location, new location will have dataSources folder structure
#         package = "" # Location overwrite for packaged files to be put in, leave blank to leave in respective dataSource location

#         [files]
#         secrets = "./secrets.toml" # Secrets file for storing sensitive information

#         [logging]
#         logToConsole = true
#         logLevel = "info" 
#     """

#     if not settingsPath.exists():
#         with open(settingsPath, "w") as fp:
#             fp.write("\n".join(line.strip() for line in _defaultSettings.split("\n")[1:]))

# def load(generateFile: bool = False) -> Settings:
#     if generateFile:
#         generate()

#     return Settings(settingsPath)

# def exists() -> bool:
#     return settingsPath.exists()