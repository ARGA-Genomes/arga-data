import lib.zipping as zp
from pathlib import Path
import scripts.ncbi.flatFileParser as ffp
from lib.processing.scripts import importableScript

@importableScript()
def parse(outputDir: Path, inputPath: Path) -> None:
    extractedFile = zp.extract(inputPath, outputDir)
    if extractedFile is None:
        return
    
    df = ffp.parseFlatfile(extractedFile)
    if df is not None:
        df.to_parquet(f"{extractedFile.stem}.parquet")
    
    extractedFile.unlink()
