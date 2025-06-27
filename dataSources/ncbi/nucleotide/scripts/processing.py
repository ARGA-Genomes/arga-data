# from lib.zipping import RepeatExtractor
import lib.zipping as zp
from pathlib import Path
from llib import flatFileParser as ffp

def parse(filePath: Path, outputFilePath: Path) -> None:
    extractedFile = zp.extract(filePath)
    df = ffp.parseFlatfile(extractedFile)
    if df is None:
        extractedFile.unlink()
        return
    
    df.to_parquet(outputFilePath)
    extractedFile.unlink()
