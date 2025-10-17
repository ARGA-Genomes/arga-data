import json
import logging
from pathlib import Path
from typing import Any
from enum import Enum
from lib.processing.files import DataFile

def translateSubsection(config: dict, subsection: str, subsectionTags: dict) -> dict:
    rawConfig = json.dumps(config)
    rawConfig = rawConfig.replace("<SUB>", subsection)

    for tag, replaceValue in subsectionTags.items():
        rawConfig = rawConfig.replace(f"<SUB:{tag.upper()}>", replaceValue)

    return json.loads(rawConfig)

class _FileProperty(Enum):
    DIR  = "DIR"
    FILE = "FILE"
    PATH = "PATH"

class FileSelect(Enum):
    INPUT    = "IN"
    OUTPUT   = "OUT"
    DOWNLOAD = "D"
    PROCESS  = "P"

class ConfigParser:
    def __init__(self, workingDir: Path, prefixLookup: dict[str, Path] = {}):
        self.workingDir = workingDir
        self.prefixLookup = prefixLookup

    def parsePath(self, arg: Any) -> Path | Any:
        if not isinstance(arg, str):
            return arg
        
        if arg.startswith("."):
            prefix, path = arg.split("/", 1)
            if prefix == ".":
                return self.workingDir / path
            
            if prefix == "..":
                cwd = self.scriptDir.parent
                while path.startswith("../"):
                    cwd = cwd.parent
                    path = path[3:]

                return cwd / path
            
            if prefix in self.prefixLookup:
                return self.imports[prefix] / path
        
        return arg
    
    def parseArg(self, arg: Any) -> Path | str:
        if not isinstance(arg, str):
            return arg
        
        if not (arg.startswith("{") and arg.endswith("}")):
            return arg
        
        parsedArg = self._parseSelectorArg(arg[1:-1])
        if  isinstance(parsedArg, str):
            logging.warning(f"Unknown key code: {parsedArg}")
            return arg
        
        return parsedArg

    def _parseSelectorArg(self, argKey: str) -> Path | str:
        if "-" not in argKey:
            logging.warning(f"Both file type and file property not present in arg, deliminate with '-'")
            return argKey
        
        fType, fProperty = argKey.split("-")

        if fType[-1].isdigit():
            selection = int(fType[-1])
            fType = fType[:-1]
        else:
            selection = 0

        fTypeEnum = FileSelect._value2member_map_.get(fType, None)
        if fTypeEnum is None:
            logging.error(f"Invalid file type: '{file}'")
            return argKey

        files = self.fileLookup.get(fTypeEnum, None)
        if files is None:
            logging.error(f"No files provided for file type: '{fType}")
            return argKey

        if selection > len(files):
            logging.error(f"File selection '{selection}' out of range for file type '{fType}' which has a length of '{len(files)}")
            return argKey
        
        file: DataFile = files[selection]
        fProperty, *suffixes = fProperty.split(".")

        if fProperty == _FileProperty.FILE.value:
            if suffixes:
                logging.warning("Suffix provided for a file object which cannot be resolved, suffix not applied")
            return file
        
        if fProperty == _FileProperty.DIR.value:
            if suffixes:
                logging.warning("Suffix provided for a parent path which cannot be resolved, suffix not applied")
            return file.path.parent

        if fProperty == _FileProperty.PATH.value:
            pth = file.path
            for suffix in suffixes:
                pth = pth.with_suffix(suffix if not suffix else f".{suffix}") # Prepend a dot for valid suffixes
            return pth
        
        logging.error(f"Unable to parse file property: '{fProperty}")
        return argKey
