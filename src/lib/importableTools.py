from pathlib import Path
from lib.processing.scripts import importableScript
from lib.processing.files import DataFile
import lib.zipping as zp
import lib.xml as xml
import lib.bigFiles as bf

@importableScript()
def extract(outputDir: Path, inputPath: Path):
    zp.extract(inputPath, outputDir)

@importableScript()
def xmlProcessor(outputDir: Path, inputPath: Path, outputFileName: str, entriesPerSection: int = 0):
    xml.basicXMLProcessor(inputPath, outputDir / outputFileName, entriesPerSection)

@importableScript(inputCount=-1, separateInputArgs=False)
def fileMerger(outputDir: Path, inputList: list[DataFile], outputFileName: str, chunkSize: int = 1024, deleteOld: bool = False):
    bf.combineDataFiles(outputDir / outputFileName, inputList)
