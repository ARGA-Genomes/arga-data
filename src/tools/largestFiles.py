from pathlib import Path
import argparse
from lib.config import globalConfig as gcfg

def getLargest(directory: Path, amountToReturn: int, includeFolders: bool, depth: int = 0) -> list[tuple[int, Path]]:
    largest = []
    smallestSize = 0

    for item in directory.iterdir():    
        if item.is_file():
            size = item.stat().st_size
            if size < smallestSize:
                continue

            largest.append((size, item))

        else:
            size = sum(file.stat().st_size for file in item.rglob("*"))
            if size < smallestSize:
                continue

            if includeFolders and depth > 3: # Within a download/processing/conversion folder
                largest.append((size, item))

            subDirLargest = getLargest(item, amountToReturn, includeFolders, depth+1)
            largest = largest + subDirLargest

        largest = sorted(largest, key=lambda x: x[0], reverse=True)[:amountToReturn]
        if largest:
            smallestSize = largest[-1][0]

    return largest

def divider():
    print("-" * 48)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find the largest files in the data sources folder")
    parser.add_argument("count", type=int, help="Amount of files to list", default=10, nargs="?")
    parser.add_argument("-f", "--folders", action="store_true", help="Include folders")

    args = parser.parse_args()

    baseDir: Path = gcfg.folders.dataSources
    largestFiles = getLargest(baseDir, args.count, args.folders)
    
    print("Largest Files")
    divider()
    for rank, (size, path) in enumerate(largestFiles, start=1):
        relativePath = path.relative_to(baseDir)

        pos = 0
        suffix = ["", "K", "M", "G", "T", "P"]
        while size > 1024:
            size = size / 1024
            pos += 1

        print(f"{rank}) {size:.02f}{suffix[pos]}B - {relativePath}")

    divider()
