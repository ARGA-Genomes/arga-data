from pathlib import Path
import json
import pandas as pd

def convert(filePath: Path, outputFilePath: Path) -> None:
    with open(filePath) as fp:
        data = json.load(fp)

    df = pd.DataFrame.from_records(data)
    df.to_csv(outputFilePath, index=False)