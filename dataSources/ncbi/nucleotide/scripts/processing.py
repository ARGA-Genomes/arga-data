from lib.zipping import RepeatExtractor
from lib.bigFileWriter import BigFileWriter
from pathlib import Path
from llib import flatFileParser as ffp

def parse(folderPath: Path, outputFilePath: Path) -> None:
    extractor = RepeatExtractor(outputFilePath.parent)
    writer = BigFileWriter(outputFilePath, "seqChunks", "chunk")

    for file in folderPath.iterdir():
        print(f"Extracting file {file.name}")
    
        extractedFile = extractor.extract(file)

        if extractedFile is None:
            print(f"Failed to extract file {file.name}, skipping")
            continue

        print(f"Parsing file {extractedFile}")
        df = ffp.parseFlatfile(extractedFile)
        writer.writeDF(df)
        extractedFile.unlink()

    writer.oneFile()
