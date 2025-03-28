from lib.zipping import RepeatExtractor
from lib.bigFileWriter import BigFileWriter
from pathlib import Path
from .. import flatFileParser as ffp

def parseNucleotide(folderPath: Path, outputFilePath: Path, verbose: bool = True) -> None:
    extractor = RepeatExtractor(outputFilePath.parent)
    writer = BigFileWriter(outputFilePath, "seqChunks", "chunk")

    for idx, file in enumerate(folderPath.iterdir(), start=1):
        if verbose:
            print(f"Extracting file {file.name}")
        else:
            print(f"Processing file: {idx}", end="\r")
    
        extractedFile = extractor.extract(file)

        if extractedFile is None:
            print(f"Failed to extract file {file.name}, skipping")
            continue

        if verbose:
            print(f"Parsing file {extractedFile}")

        df = ffp.parseFlatfile(extractedFile, verbose)
        writer.writeDF(df)
        extractedFile.unlink()

    writer.oneFile()
