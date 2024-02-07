import lib.config as cfg
import json
from pathlib import Path

if __name__ == "__main__":
    updatesFile = cfg.folders.src / "lib" / "scheduling" / "sourceUpdates.json"
    defaultConfig = {
        "updateType": "weekly",
        "updateValue": "sunday",
        "repeatInterval": 2,
        "time": 9,
        "method": "full"
    }

    with open(updatesFile) as fp:
        currentData: dict[str, dict[str, str]] = json.load(fp)

    dataSources: Path = cfg.folders.datasources
    for location in dataSources.iterdir():
        for database in location.iterdir():
            if database.is_file(): # Invalid file in location folder
                continue

            if "config.json" not in [path.name for path in database.iterdir()]:
                print(f"Skipped {location.stem}-{database.stem}")
                continue

            if location.stem not in currentData:
                currentData[location.stem] = {}

            if database.stem not in currentData[location.stem]:
                currentData[location.stem][database.stem] = defaultConfig

        if location.stem in currentData:
            currentData[location.stem] = {k: v for k, v in sorted(currentData[location.stem].items(), key=lambda x: x[0])}
    
    currentData = {k: v for k, v in sorted(currentData.items(), key=lambda x: x[0])}

    with open(updatesFile, "w") as fp:
        json.dump(currentData, fp, indent=4)