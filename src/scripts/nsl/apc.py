from pathlib import Path
import lib.common as cmn
from lib.processing.scripts import importableScript
from lib.processing.files import DataFile

@importableScript()
def denormalize(outputDir: Path, inputFile: DataFile) -> None:
    df = inputFile.read()

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
    df.to_csv(outputDir / "denormalized.csv", index=False)

@importableScript()
def cleanup(outputDir: Path, inputFile: DataFile) -> None:
    df = inputFile.read()
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
    
    df.to_csv(outputDir / "apc.csv", index=False)
