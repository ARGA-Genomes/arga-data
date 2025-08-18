import pandas as pd

def augment(df: pd.DataFrame) -> pd.DataFrame:
    df["collections", "scientificName"] = "Phascolarctos cinereus"
    return df