from pathlib import Path
import pandas as pd

def getSheetNames(xlPath: Path) -> list[str]:
    xls = pd.ExcelFile(xlPath)
    return xls.sheet_names

def loadSheet(xlPath: Path, sheetName: str) -> pd.DataFrame:
    xls = pd.ExcelFile(xlPath)
    if sheetName not in xls.sheet_names:
        print(f"No sheet found in {xlPath} with name: '{sheetName}'")
        return None
    
    return xls.parse(sheetName)
