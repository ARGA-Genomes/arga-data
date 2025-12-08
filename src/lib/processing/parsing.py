import logging
from pathlib import Path
from enum import Enum
from lib.processing.files import DataFile
from typing import Any

class FileSelect(Enum):
    INPUT    = "IN"
    OUTPUT   = "OUT"
    DOWNLOAD = "D"
    PROCESS  = "P"

class FileProperty(Enum):
    DIR  = "DIR"
    FILE = "FILE"
    PATH = "PATH"

class DataFileLookup:
    def __init__(self, inputs: list[DataFile] = [], outputs: list[DataFile] = [], downloads: list[DataFile] = [], processed: list[DataFile] = []):
        self._enumMap = {
            FileSelect.INPUT: inputs,
            FileSelect.OUTPUT: outputs,
            FileSelect.DOWNLOAD: downloads,
            FileSelect.PROCESS: processed
        }

    def getFiles(self, enum: FileSelect) -> list[DataFile]:
        return self._enumMap.get(enum, [])
    
    def merge(self, lookup: 'DataFileLookup') -> None:
        for enum in FileSelect:
            self._enumMap[enum] += lookup._enumMap[enum]

class DirLookup:
    def __init__(self, directories: list[Path] = []):
        self._lookup = {f".{directory.name}": directory for directory in directories}

    def contains(self, prefix: str) -> bool:
        return prefix in self._lookup

    def remap(self, path: Path, prefix: str) -> Path:
        return self._lookup[prefix] / path

def parseDict(data: dict, relativeDir: Path, dirLookup: DirLookup = DirLookup(), dataFileLookup: DataFileLookup = DataFileLookup()):
    res = {}
    for key, value in data.items():
        if isinstance(value, list):
            res[key] = [parseArg(arg, relativeDir, dirLookup, dataFileLookup) for arg in value]

        elif isinstance(value, dict):
            res[key] = parseDict(value, relativeDir, dirLookup, dataFileLookup)

        else:
            res[key] = parseArg(value, relativeDir, dirLookup, dataFileLookup)

def parseArg(arg: Any, parentDir: Path, dirLookup: DirLookup = DirLookup(), dataFileLookup: DataFileLookup = DataFileLookup()) -> Path | str:
    if not isinstance(arg, str):
        return arg

    if arg.startswith("."):
        return parsePath(arg, parentDir, dirLookup)
    
    if arg.startswith("{") and arg.endswith("}"):
        parsedArg = _parseSelectorArg(arg[1:-1], dataFileLookup)
        if parsedArg == arg:
            logging.warning(f"Unknown key code: {parsedArg}")

        return parsedArg

    return arg

def parsePath(arg: str, parentPath: Path, dirLookup: DirLookup = DirLookup()) -> Path | Any:
    prefix, relPath = arg.split("/", 1)
    if prefix == ".":
        return parentPath / relPath
        
    if prefix == "..":
        cwd = parentPath.parent
        while relPath.startswith("../"):
            cwd = cwd.parent
            relPath = relPath[3:]

        return cwd / relPath
    
    if dirLookup.contains(prefix):
        return dirLookup.remap(relPath, prefix)
    
    return arg

def _parseSelectorArg(arg: str, dataFileLookup: DataFileLookup = DataFileLookup()) -> Path | str:
    if "-" not in arg:
        logging.warning(f"Both file type and file property not present in arg, deliminate with '-'")
        return arg
    
    fType, fProperty = arg.split("-")

    if fType[-1].isdigit():
        selection = int(fType[-1])
        fType = fType[:-1]
    else:
        selection = 0

    fTypeEnum = FileSelect._value2member_map_.get(fType, None)
    if fTypeEnum is None:
        logging.error(f"Invalid file type: '{file}'")
        return arg

    files = dataFileLookup.getFiles(fTypeEnum)
    if not files:
        logging.error(f"No files provided for file type: '{fType}")
        return arg

    if selection > len(files):
        logging.error(f"File selection '{selection}' out of range for file type '{fType}' which has a length of '{len(files)}")
        return arg
    
    file: DataFile = files[selection]
    fProperty, *suffixes = fProperty.split(".")

    if fProperty == FileProperty.FILE.value:
        if suffixes:
            logging.warning("Suffix provided for a file object which cannot be resolved, suffix not applied")
        return file
    
    if fProperty == FileProperty.DIR.value:
        if suffixes:
            logging.warning("Suffix provided for a parent path which cannot be resolved, suffix not applied")
        return file.path.parent

    if fProperty == FileProperty.PATH.value:
        pth = file.path
        for suffix in suffixes:
            pth = pth.with_suffix(suffix if not suffix else f".{suffix}") # Prepend a dot for valid suffixes
        return pth
    
    logging.error(f"Unable to parse file property: '{fProperty}")
    return arg
