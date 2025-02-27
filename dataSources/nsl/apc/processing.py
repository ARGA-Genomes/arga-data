from pathlib import Path
import pandas as pd
import lib.commonFuncs as cmn

def denormalize(filePath: Path, outputFilePath: Path) -> None:
    df = pd.read_csv(filePath)
    columnMap = {column: cmn.toSnakeCase(column) for column in df.columns}
    df = df.rename(columns=columnMap)

    subDFMap = {
        "taxon_id": "parent_taxon_id",
        "scientific_name": "parent_taxon",
        "taxon_rank": "parent_rank"
    }

    df2 = df[subDFMap.keys()]
    df2 = df2.rename(columns=subDFMap)

    df = df.merge(df2, "left", left_on="parent_name_usage_id", right_on="parent_taxon_id")
    df.to_csv(outputFilePath, index=False)

def cleanup(filePath: Path, outputFilePath: Path) -> None:
    df = pd.read_csv(filePath)
    df = df.replace(
        {
            "[unranked]": "unranked",
            "[n/a]": "unranked",
            "[infraspecies]": "infraspecies",
            "[infragenus]": "infragenus"
        }
    )
    
    datasetID = "ARGA:TL:0001008"
    df["dataset_id"] = datasetID
    df["entity_id"] = f"{datasetID};" + df["scientific_name"]
    
    df.to_csv(outputFilePath, index=False)
