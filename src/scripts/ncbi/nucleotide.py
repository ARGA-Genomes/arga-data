# from lib.zipping import RepeatExtractor
import lib.zipping as zp
from pathlib import Path
from llib import flatFileParser as ffp

def parse(filePath: Path, outputFilePath: Path) -> None:
    extractedFile = zp.extract(filePath, outputFilePath.parent)
    if extractedFile is None:
        return
    
    df = ffp.parseFlatfile(extractedFile)
    if df is None:
        extractedFile.unlink()
        return
    
    df.to_parquet(outputFilePath)
    extractedFile.unlink()
