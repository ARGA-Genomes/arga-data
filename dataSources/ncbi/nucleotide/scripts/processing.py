from lib.zipping import RepeatExtractor
from lib.bigFiles import DFWriter
from pathlib import Path
from llib import flatFileParser as ffp
import lib.dataframes as dff

def parseNucleotide(folderPath: Path, outputFilePath: Path, verbose: bool = True) -> None:
    extractor = RepeatExtractor(outputFilePath.parent)
    writer = DFWriter(outputFilePath)

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
        writer.write(df)
        extractedFile.unlink()

    writer.combine(True)
