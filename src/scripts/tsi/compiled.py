from pathlib import Path
import lib.downloading as dl
from lib.processing.scripts import importableScript
import pandas as pd

@importableScript(inputCount=0)
def collectFromSheets(outputDir: Path):
    documentID = "1xy15ARqq8_0WRmTY7xcMz3giBwfs0iFOU6dMWjQN-oo"
    sheetTabs = {
        "livestate": 1334260048,
        "organisms": 0,
        "collecting": 1174387857,
        "tissues": 1559676056,
        "lab_samples": 2130570893,
        "data_products": 1700782579
    }

    df = dl.getGoogleSheet(documentID, sheetTabs["organisms"])

    def mergeSheet(df: pd.DataFrame, sheetName: str, mergeLeft: str, mergeRight: str = "") -> pd.DataFrame:
        df2 = dl.getGoogleSheet(documentID, sheetTabs[sheetName])
        colDiff = df2.columns.difference(df.columns).to_list()
        if not mergeRight:
            return df.merge(df2[[mergeLeft] + colDiff], "left", mergeLeft)
        return df.merge(df2[colDiff], "left", left_on=mergeLeft, right_on=mergeRight)

    df = mergeSheet(df, "livestate", "organism:organism_id")
    df = mergeSheet(df, "collecting", "organism:organism_id", "collecting:organism_id")
    df = mergeSheet(df, "tissues", "organism:organism_id", "tissues:organism_id")
    df = mergeSheet(df, "lab_samples", "tissues:tissue_id")
    df = mergeSheet(df, "data_products", "lab_sample:extract_id")

    df.to_csv(outputDir / "tsiCompiled.csv", index=False)
