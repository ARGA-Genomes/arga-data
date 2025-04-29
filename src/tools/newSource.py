import lib.config as cfg
import argparse
from lib.data.sources import SourceManager
from pathlib import Path
import json
from lib.data.database import Retrieve

def _urlConfig() -> dict:
    return {
        "retrieveType": "url",
        "downloading": [
            {
                "url": "",
                "name": ""
            }
        ],
        "conversion": {}
    }

def _crawlConfig() -> dict:
    return {
        "retrieveType": "crawl",
        "downloading": {
            "url": "",
            "regex": ""
        },
        "conversion": {}
    }

def _scriptConfig() -> dict:
    return {
        "retrieveType": "script",
        "downloading": {
            "path": "",
            "function": "",
            "args": [],
            "output": []
        },
        "conversion": {}
    }

def _defaultConfig() -> dict:
    return {
        "retrieveType": "",
        "downloading": {},
        "conversion": {}
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate folders for a new data source")
    parser.add_argument("location", help="Location of the data source")
    parser.add_argument("database", help="Database name for location")
    parser.add_argument("-t", "--type", choices=list(Retrieve._value2member_map_), help="Extra setup if db type is known")
    args = parser.parse_args()

    sourceManager = SourceManager()
    locationFolder: Path = cfg.Folders.dataSources / args.location
    databaseFolder: Path = locationFolder / args.database

    if databaseFolder.exists():
        print(f"Database {args.location}-{args.database} already exists, exiting...")
        exit()

    if not locationFolder.exists():
        print(f"Creating new location folder for: {args.location}")
        locationFolder.mkdir()
    else:
        print(f"Location '{args.location}' already exists, creating within")

    print(f"Creating database folder: {args.database}")
    databaseFolder.mkdir()

    # Setting up database folder
    dataFolder = databaseFolder / "data"
    dataFolder.mkdir()

    configs = {
        Retrieve.URL: _urlConfig,
        Retrieve.CRAWL: _crawlConfig,
        Retrieve.SCRIPT: _scriptConfig,
    }

    retriveType = Retrieve._value2member_map_.get(args.type, None)
    config = configs.get(retriveType, _defaultConfig)

    configFile = databaseFolder / "config.json"
    with open(configFile, "w") as fp:
        json.dump(config(), fp, indent=4)
