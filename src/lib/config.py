from lib.tomlFiles import TomlLoader, Any
from pathlib import Path

class Configs(TomlLoader):
    def parse(self, value: any) -> Any:
        if isinstance(value, str):
            if value.startswith("./"):
                return self._path.parent / value[2:]

            if value.startswith("/") or value[1:].startswith(":/"):
                return Path(value)
        
        return value

globalConfig = Configs(Path(__file__).parents[2] / "config.toml")