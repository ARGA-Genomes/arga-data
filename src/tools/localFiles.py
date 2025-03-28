import lib.config as cfg
from pathlib import Path
import zipfile

def getDatabaseFiles(folderPath: Path) -> list[Path]:
    localFiles = []
    for item in folderPath.iterdir():
        if item.name in ("data", "examples", "crawlerProgress", "__pycache__"):
            continue

        if not item.is_file():
            localFiles.extend(getDatabaseFiles(item))
            continue

        if item.suffix == ".py" or item.suffix == ".pyc" or item.name in ("config.json", "metadata.json", "crawl.txt"):
            continue

        localFiles.append(item)

    return localFiles

if __name__ == "__main__":
    data: Path = cfg.Folders.dataSources
    localFilesFolder = data.parent / "localFiles.zip"

    if localFilesFolder.exists():
        print("Local files zipfile already exists, please move or delete before running again, exiting...")

    localFiles: list[Path] = []
    for location in data.iterdir():
        for database in location.iterdir():
            if database.is_file():
                continue

            localFiles.extend(getDatabaseFiles(database))
    
    with zipfile.ZipFile(localFilesFolder, "w", zipfile.ZIP_DEFLATED) as zipfp:
        for file in localFiles:
            zipfp.write(file, file.relative_to(data.parent))
    