from lib.tomlFiles import TomlLoader, Any
from pathlib import Path

_defaultSettings = """
[folders]
logs = "./logs" # Location of all logging files, cannot overwrite with local config files

[storage]
data = "" # Location overwrite for source data including downloading/processing/conversion, leave blank to keep in respective dataSource location, new location will have dataSources folder structure
package = "" # Location overwrite for packaged files to be put in, leave blank to leave in respective dataSource location

[files]
secrets = "./secrets.toml" # Secrets file for storing sensitive information

[logging]
logToConsole = true
logLevel = "info" # Log levels: debug, info, warning, error, critical
"""

class Settings(TomlLoader):
    def parse(self, value: any) -> Any:
        if isinstance(value, str):
            if value.startswith("./"):
                return self._path.parent / value[2:]

            if value.startswith("/") or value[1:].startswith(":/"):
                return Path(value)
        
        return value

rootDir = Path(__file__).parents[2]
dataSourcesDir = rootDir / "dataSources"

_settingsPath = rootDir / "settings.toml"
if not _settingsPath.exists():
    with open(_settingsPath, "w") as fp:
        fp.write(_defaultSettings.replace("\n    ", "\n").strip("\n"))

globalSettings = Settings(_settingsPath)
