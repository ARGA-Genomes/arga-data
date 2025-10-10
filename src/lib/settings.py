from lib.tomlFiles import TomlLoader, Any
from pathlib import Path

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
globalSettings = Settings(rootDir / "settings.toml")
