import pandas as pd
import lib.dataframes as dff
import lib.common as cmn

def augmentBiosample(df: pd.DataFrame) -> pd.DataFrame:
    return dff.splitField(df, "ncbi_lat long", cmn.latlongToDecimal, ["decimalLatitude", "decimalLongitude"])
