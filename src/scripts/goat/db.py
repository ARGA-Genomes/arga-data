import requests
from urllib.parse import quote
import pandas as pd
from pathlib import Path
import lib.downloading as dl
from lib.processing.scripts import importableScript
from lib.processing.files import DataFile

@importableScript(inputCount=0)
def build(outputDir: Path) -> None:
    query = "tax_name(*) AND tax_rank(species)"

    def buildCall(size: int, tidy: bool) -> str:
        return f"https://goat.genomehubs.org/api/v2/search?query={quote(query)}&result=taxon&includeEstimates=true&size={size}&tidyData={'true' if tidy else 'false'}"

    response = requests.get(buildCall(0, False))
    output = response.json()
    status = output.get("status", {})
    hits = status.get("hits", 0)

    dl.download(buildCall(hits, True), outputDir / "goat.csv", headers={"accept": "text/csv"})

@importableScript()
def clean(outputDir: Path, inputFile: DataFile) -> None:
    df = inputFile.read()

    df["aggregation_source"] = df["aggregation_source"].apply(lambda x: x.replace('"', ''))
    combineFields = ["field", "value", "aggregation_source", "aggregation_method"]
    df["data"] = df[combineFields].apply(lambda x: {col: item for col, item in zip(combineFields, x)}, axis=1)
    data = df.groupby("taxon_id")[["data"]].agg(lambda x: [v for v in x])
    df.drop(combineFields + ["data"], axis=1, inplace=True)
    df = df.merge(data, "left", on="taxon_id")
    df = df.drop_duplicates(["taxon_id"])
    df.to_csv(outputDir / "cleanedGoat.csv", index=False)
