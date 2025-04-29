from pathlib import Path
import toml
from enum import Enum

class ConfigType(Enum):
    FOLDERS = "folders"
    SETTINGS = "settings"
        
class ConfigMeta(type):
    def __new__(cls: type, name: str, bases: tuple[str], attrs: dict):
        rootDir = Path(__file__).parents[2]
        with open(rootDir / "config.toml") as fp:
            data = toml.load(fp)

        cfgType = attrs.get("cfg", None)
        if cfgType is None:
            raise AttributeError from Exception(f"No parameter `cfg` defined on object as required")

        cfgValue = cfgType.value
        items: dict | None = data.get(cfgValue, None)
        if items is None:
            raise AttributeError from Exception(f"Invalid config item: {cfgValue}")
        
        for k, v in items.items():
            if isinstance(v, str):
                if v.startswith("./"):
                    v = rootDir / Path(v)
                elif v.startswith("/"):
                    v = Path(v)
                    
            attrs[k] = v

        return super().__new__(cls, name, bases, attrs)

class Folders(metaclass=ConfigMeta):
    cfg = ConfigType.FOLDERS

class Settings(metaclass=ConfigMeta):
    cfg = ConfigType.SETTINGS
