from pathlib import Path
import json
import pandas as pd
from lib.processing.scripts import importableScript

@importableScript()
def convert(outputDir: Path, inputPath: Path) -> None:
    with open(inputPath) as fp:
        data = json.load(fp)

    df = pd.DataFrame.from_records(data)
    df.to_csv(outputDir / "rapidRelease.csv", index=False)
