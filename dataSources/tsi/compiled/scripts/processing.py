from pathlib import Path
import pandas as pd

def manualCollect(sheetsID: str, specificSheetID: str, outputFilePath: Path) -> None:
    sheetURL = f"https://docs.google.com/spreadsheets/d/{sheetsID}/export?format=csv&gid={specificSheetID}"
    df = pd.read_csv(sheetURL, keep_default_na=False, skiprows=[x for x in range(23) if x != 4])
    df = df.drop(["ARGA_DwC"], axis=1)
    df.to_csv(outputFilePath, index=False)
