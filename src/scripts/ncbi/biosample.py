import pandas as pd
from pathlib import Path
import lib.zipping as zp
import lib.xml as xml
from lib.processing.scripts import importableScript

@importableScript()
def parse(outputDir: Path, inputPath: Path):
    extractedFile = zp.extract(inputPath, outputDir)

    xmlOutput = outputDir / "rawBiosample.csv"
    xml.basicXMLProcessor(extractedFile, xmlOutput, 150000)

    df = pd.read_csv(xmlOutput)
    df[["decimalLatitude", "decimalLongitude"]] = df["ncbi_lat long"].str.split(" ", expand=True)
    df = df.drop("ncbi_lat long", axis=1)
    df.to_csv(outputDir / "biosample.csv", index=False)

    extractedFile.unlink()
    xmlOutput.unlink()
