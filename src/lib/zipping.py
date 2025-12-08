import zipfile
import shutil
import gzip
from pathlib import Path
import logging

def _gunzip(gzippedFile: Path, outputfilePath: Path) -> None:
    with gzip.open(gzippedFile, "rb") as fpIn:
        with open(outputfilePath, "wb") as fpOut:
            shutil.copyfileobj(fpIn, fpOut)


extractableSuffixes = (".zip", ".tar", ".gz", ".xz", ".bz2")
try:
    shutil.register_unpack_format("gz", [".gz"], _gunzip)
except shutil.RegistryError:
    pass

class RepeatExtractor:
    def __init__(self, outputDir: str = "", addSuffix: str = "", overwrite: bool = False):
        self.outputDir = outputDir
        self.addSuffix = addSuffix
        self.overwrite = overwrite

    def extract(self, filePath: str) -> Path | None:
        return extract(filePath, self.outputDir, self.addSuffix, self.overwrite)

def extract(filePath: Path, outputDir: Path = None, addSuffix: str = "", overwrite: bool = False, verbose: bool = True) -> Path | None:
    if not filePath.exists():
        logging.warning(f"No file exists at path: {filePath}.")
        return
    
    if outputDir is None:
        outputDir = filePath.parent
    
    outputPath = extractsTo(filePath, outputDir, addSuffix)
    if outputPath.exists() and not overwrite:
        if verbose:
            logging.info(f"Output {outputPath.name} exists, skipping extraction stage")
            
        return outputPath

    if verbose:
        logging.info(f"Extracting {filePath} to {outputPath}")

    try:
        shutil.unpack_archive(filePath, outputPath)
    except:
        outputPath.unlink(missing_ok=True)
        return

    return outputPath

def compress(filePath: Path, outputDir: Path = None, zipName: str = None, includeFolder: bool = True) -> Path | None:
    def compressFolder(folderPath: Path, parentFolder: Path, fp: zipfile.ZipFile):
        for item in folderPath.iterdir():
            itemPath = Path(parentFolder, item.name)
            if item.is_file():
                fp.write(item, itemPath if includeFolder else itemPath.name)
            else:
                compressFolder(item, itemPath, fp)

    if zipName is None:
        zipName = filePath.stem
    if outputDir is None:
        outputDir = filePath.absolute().parent

    outputFile = outputDir / f"{zipName}.zip"

    with zipfile.ZipFile(outputFile, "w", zipfile.ZIP_DEFLATED) as zipfp:
        if filePath.is_file():
            zipfp.write(filePath, outputFile.stem if includeFolder else outputFile.name)
        else:
            compressFolder(filePath, outputFile.stem, zipfp)
    
    return outputFile

def canBeExtracted(filePath: Path) -> bool:
    return any(suffix in extractableSuffixes for suffix in filePath.suffixes)

def extractsTo(filePath: Path, outputDir: Path = None, addSuffix: str = "") -> Path:
    outputPath = outputDir / filePath.name[:-len("".join(suffix for suffix in filePath.suffixes if suffix in extractableSuffixes))]
    if addSuffix:
        outputPath = outputPath.with_suffix(addSuffix)
    
    return outputPath
