from lib.tomlFiles import TomlLoader
from pathlib import Path

secrets = TomlLoader(Path(__file__).parents[2] / "secrets.toml")
