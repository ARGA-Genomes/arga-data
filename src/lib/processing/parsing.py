import logging
from pathlib import Path
from enum import Enum
from lib.processing.files import DataFile
from typing import Any

def parsePath(arg: str, relativeDir: Path, dirLookup: dict[str, Path]) -> Path | Any:
    prefix, relPath = arg.split("/", 1)
    if prefix in dirLookup:
        return dirLookup[prefix] / relPath
    
    if len(prefix) == 2 and prefix[0].isupper() and prefix[1] == ":":
        return Path(arg)

    if prefix == ".":
        return relativeDir / relPath
        
    if prefix == "..":
        cwd = relativeDir.parent
        while relPath.startswith("../"):
            cwd = cwd.parent
            relPath = relPath[3:]

        return cwd / relPath
    
    return arg

def parseInput(arg: str, downloaded: list[list[DataFile]], processed: list[list[DataFile]]) -> list[DataFile | Path | str]:
    asPath = arg.endswith("_") # Return outputs as path insteasd
    fileInfo = arg.rstrip("_")
    
    # File selection parsing
    outputNumbers = []
    def _addSelection(value: int) -> None:
        if value in outputNumbers:
            logging.warning("Duplicate input added to datafile selection")
            return

        outputNumbers.append(value)

    if "|" in fileInfo: # Output number selection
        fileInfo, rawSelection = arg.split("|")

        for selectionItem in rawSelection.split(","):
            if "-" not in selectionItem: # No range selection
                _addSelection(int(selectionItem))
                continue

            start, end = selectionItem.split("-")
            for subselectionItem in range(int(start), int(end)+1):
                _addSelection(subselectionItem)

    # Lookup parsing
    lookupSelection = fileInfo[0]
    if lookupSelection == "D":
        lookup = downloaded
    elif lookupSelection == "P":
        lookup = processed
    else:
        logging.error(f"Invalid lookup selection: {lookupSelection}")
        return arg

    # Lookup step parsing
    stepSelection = fileInfo[1:]
    selectedFile = lookup[-1] if stepSelection == "^" else lookup[int(stepSelection)]

    # Lookup step output selection
    outputs = []
    for number in outputNumbers:
        outputs.append(selectedFile[number].path if asPath else selectedFile[number])

    return outputs

def parseInputList(inputArgs: list[str], downloads: list[list[DataFile]], processed: list[list[DataFile]]) -> list[DataFile | Path | str]:
    return [file for arg in inputArgs for file in parseInput(arg, downloads, processed)]
