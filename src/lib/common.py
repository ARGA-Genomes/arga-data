import csv
import json
import pandas as pd
from typing import Generator
from pathlib import Path

def reverseLookup(lookup: dict) -> dict:
    nameMap = {}

    for newName, oldNameList in lookup.items():
        for name in oldNameList:
            if name not in nameMap:
                nameMap[name]  = []
            
            nameMap[name].append(newName)

    return nameMap

def flatten(inputDict: dict, parent: str = "") -> dict:
    res = {}
    
    for key, value in inputDict.items():
        addKey = key if not parent else f"{parent}_{key}"

        if isinstance(value, dict):
            res.update(flatten(value, parent=key))
        elif isinstance(value, list) or isinstance(value, tuple):
            for item in value:
                if isinstance(item, dict):
                    res.update(flatten(item, parent=key))
                else:
                    res[addKey] = item
        else:
            res[addKey] = value

    return res

def chunkGenerator(filePath: str, chunkSize: int, sep: str = ",", header: int = 0, encoding: str = "utf-8", usecols: list = None, nrows: int = None) -> Generator[pd.DataFrame, None, None]:
    return (chunk for chunk in pd.read_csv(filePath, on_bad_lines="skip", chunksize=chunkSize, sep=sep, header=header, encoding=encoding, dtype=object, usecols=usecols, nrows=nrows))

def getColumns(filePath: str, separator: str = ',', headerRow: int = 0) -> str:
    with open(filePath, encoding='utf-8') as fp:
        reader = csv.reader(fp, delimiter=separator)
        for idx, row in enumerate(reader):
            if idx == headerRow:
                return row

def extendUnique(lst: list, values: list) -> list[any]:
    return lst + [value for value in values if value not in lst]

def addUniqueEntry(dictionary: dict, key: any, value: any, duplicateLimit: int = 0) -> None:
    if key not in dictionary:
        dictionary[key] = value
        return

    suffixNum = 1
    while suffixNum != duplicateLimit:
        newKey = f"{key}_{suffixNum}"
        if newKey not in dictionary:
            dictionary[newKey] = value
            return
        suffixNum += 1

def loadFromJson(path: str) -> dict:
    with open(path) as fp:
        return json.load(fp)

def dictListToCSV(dictList: list, columns: list, filePath: str) -> None:
    with open(filePath, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.DictWriter(fp, columns)
        writer.writeheader()

        for d in dictList:
            writer.writerow(d)

def clearFolder(folderPath: Path, delete: bool = False) -> None:
    if not folderPath.exists():
        return
    
    for item in folderPath.iterdir():
        if item.is_file():
            item.unlink()
        else:
            clearFolder(item, True)
    
    if delete:
        folderPath.rmdir()

def toSnakeCase(s: str) -> str:
    ret = ""
    upperCount = 0
    for char in s:
        if char.isalpha() and char.upper() == char:
            if upperCount == 0:
                ret += "_"
            ret += char.lower()
            upperCount += 1
        else:
            upperCount = 0
            ret += char

    return ret