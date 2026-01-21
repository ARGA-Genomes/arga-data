from lib.tomlFiles import TomlLoader
import lib.settings as settings
from pathlib import Path
import logging

def load() -> TomlLoader:
    globalSettings = settings.load()
    filePath = globalSettings.files.secrets
    
    if not isinstance(filePath, Path):
        logging.error(f"Invalid secrets filepath: {filePath}")
        exit()

    if not filePath.exists():
        logging.error(f"No secrets file found at {filePath}")
        exit()

    return TomlLoader(globalSettings.files.secrets)