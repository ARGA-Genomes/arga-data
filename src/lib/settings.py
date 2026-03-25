import toml
from pathlib import Path
import lib.processing.parsing as parsing

class Settings:

    class Folders:
        LOGS = "./logs"

    class Storage:
        DATA = ""
        PACKAGE = ""

    class Files:
        SECRETS = "./secrets.toml"

    class Logging:
        LOG_TO_CONSOLE = True
        LOG_LEVEL = "info" 

    def __init__(self, loadVariables: bool = True):
        self.rootDir = Path(__file__).parents[2]
        self.dataSourcesDir = self.rootDir / "dataSources"
        self.srcDir = self.rootDir / "src"
        self.libDir = self.srcDir / "lib"
        self.scriptsDir = self.srcDir / "scripts"
        self.settingsPath = self.rootDir / "settings.toml"

        if loadVariables:
            self._load()

    def _generate(self):
        comments = {
            "LOGS": "Location of all logging files, cannot overwrite with local config files",
            "DATA": "Location overwrite for source data including downloading/processing/conversion, leave blank to keep in respective dataSource location, new location will have dataSources folder structure",
            "PACKAGE": "Location overwrite for packaged files to be put in, leave blank to leave in respective dataSource location",
            "SECRETS": "Secrets file for storing sensitive information",
            "LOG_LEVEL": "Log levels: debug, info, warning, error, critical"
        }

        with open(self.settingsPath, "w") as fp:
            for item in (self.Folders, self.Storage, self.Files, self.Logging):

                fp.write(f"[{item.__name__.lower()}]\n")

                for property in vars(item):
                    if not property.isupper():
                        continue

                    value = getattr(item, property)
                    comment = comments.get(property, "")

                    fp.write(f"{property.lower()} = ")
                    fp.write(str(value).lower() if not isinstance(value, str) else f"\"{value}\"")
                             
                    if comment:
                        fp.write(f"# {comment}")

                    fp.write("\n")
                
                fp.write("\n")

    def _load(self) -> None:
        if not self.settingsPath.exists():
            return self._generate()
        
        self.update(self.settingsPath)

    def update(self, path: Path) -> None:
        with open(path) as fp:
            data = toml.load(fp)

        for key, pair in data.items():
            for variable, value in pair.items():
                subClass = getattr(self, key.capitalize())
                setattr(subClass, variable.upper(), parsing.parseArg(value, path.parent))
