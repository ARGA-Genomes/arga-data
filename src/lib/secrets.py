from lib.tomlFiles import TomlLoader
from lib.settings import globalSettings as gs

secrets = TomlLoader(gs.files.secrets)
