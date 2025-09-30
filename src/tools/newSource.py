from lib.settings import dataSourcesDir
import argparse
from lib.data.sources import sourceConfigName
from pathlib import Path
import json
from lib.data.database import Retrieve

urlDLConfig = [
    {
        "url": "",
        "name": ""
    }
]

crawlDLConfig = {
    "url": "",
    "regex": "",
    "urlPrefix": ""
}

scriptDLConfig = {
    "path": "",
    "function": "",
    "args": [],
    "output": ""
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate folders for a new data source")
    parser.add_argument("location", help="Location of the data source")
    parser.add_argument("database", help="Database name for location")
    parser.add_argument("type", choices=list(Retrieve._value2member_map_), help="Type of database to set up")
    args = parser.parse_args()

    locationFolder: Path = dataSourcesDir / args.location
    databaseFolder: Path = locationFolder / args.database
    configFilePath: Path = databaseFolder / sourceConfigName

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

    dlconfigs = {
        Retrieve.URL: urlDLConfig,
        Retrieve.CRAWL: crawlDLConfig,
        Retrieve.SCRIPT: scriptDLConfig,
    }

    retriveType = Retrieve._value2member_map_[args.type]
    config = {
        "retrieveType": retriveType.value,
        "datasetID": "",
        "downloading": dlconfigs[retriveType],
        "update": {
            "type": "weekly",
            "day": "sunday",
            "time": 9,
            "repeat": 2
        }
    }

    with open(configFilePath, "w") as fp:
        json.dump(config, fp, indent=4)
