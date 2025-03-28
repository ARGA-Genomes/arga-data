import lib.common as cmn
from pathlib import Path

baseDir = Path(__file__)
sourceFolder = baseDir / "inaturalist-open-data-20230827"
outputFolder = baseDir / "chunks"

chunkSize = 1000

if not outputFolder.exists():
    outputFolder.mkdir()

for file in sourceFolder.iterdir():
    chunkGen = cmn.chunkGenerator(file, chunkSize, sep="\t")
    chunk = next(chunkGen)

    chunk.to_csv(outputFolder / f"{file.stem}_chunk.csv", index=False)
