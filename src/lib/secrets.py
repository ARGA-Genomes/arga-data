from lib.tomlFiles import TomlLoader
from lib.settings import globalSettings as gs
from pathlib import Path
import logging

filePath = gs.files.secrets
if not isinstance(filePath, Path):
    logging.error(f"Invalid secrets filepath: {filePath}")
    exit()

if not filePath.exists():
    logging.error(f"No secrets file found at {filePath}")
    exit()

secrets = TomlLoader(gs.files.secrets)
