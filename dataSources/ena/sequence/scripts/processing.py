from pathlib import Path
import lib.zipping as zp
import sequence.scripts.parser as parser

def parse(inputPath: Path, outputPath: Path):
    extractedFile = zp.extract(inputPath, outputPath.parent)
    if extractedFile is None:
        return
    
    parser.parseFile(extractedFile, outputPath)
    extractedFile.unlink()
