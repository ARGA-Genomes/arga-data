from lib.tomlFiles import TomlLoader
from lib.config import globalConfig as gcfg

secrets = TomlLoader(gcfg.files.secrets)
