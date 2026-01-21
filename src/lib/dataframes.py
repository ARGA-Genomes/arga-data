import pandas as pd
import numpy as np
import pandas as pd

def getColumnEmptyCount(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return -1

    return len(df[df[column].isna()])

def getColumnCount(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return -1

    return len(df[df[column].notna()])

def dropEmptyColumns(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    remainingColumns = set(df.columns)
    df.dropna(how='all', axis=1, inplace=True) # Drop all completely empty columns

    if verbose:
        for column in remainingColumns.symmetric_difference(df.columns): # Iterate over removed column names
            print(f"Dropped empty column {column}")

    return df

def applyExclusions(df: pd.DataFrame, exclusionMap: dict) -> pd.DataFrame:
    for excludeType, properties in exclusionMap.items():
        dwcName = properties["dwc"]
        exclusions = properties["data"]
        if dwcName in df.columns: # Make sure dwc field exists in file
            df = df.drop(df[df[dwcName].isin(exclusions)].index)
    
    return df

def removeSpaces(df: pd.DataFrame) -> pd.DataFrame:
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    return df
