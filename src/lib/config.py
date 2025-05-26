from lib.tomlFiles import TomlLoader, Any
from pathlib import Path

class Configs(TomlLoader):
    def __init__(self, path: Path):
        super().__init__(path)

        for attrDict in self._data.values():
            for key, value in attrDict._data.items():
                if isinstance(value, str):
                    if value.startswith("./"):
                        value = path.parent / value[2:]

                    elif value.startswith("/") or value[1:].startswith(":/"):
                        value = Path(value)

                attrDict._data[key] = value

globalConfig = Configs(Path(__file__).parents[2] / "config.toml")
