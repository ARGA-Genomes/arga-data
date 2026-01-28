from pathlib import Path
import lib.downloading as dl
from lib.processing.scripts import importableScript

@importableScript(inputCount=0)
def collectFromSheets(outputDir: Path):
    documentID = "1xy15ARqq8_0WRmTY7xcMz3giBwfs0iFOU6dMWjQN-oo"
    
    sheetTabs = {
        "organisms": 0,
        "collecting": 1174387857,
        "tissues": 1559676056,
        "lab_samples": 2130570893
    }

    outputDir.mkdir(exist_ok=True)
    for fileName, sheetID in sheetTabs.items():
        df = dl.getGoogleSheet(documentID, sheetID)
        df.to_csv(outputDir / f"{fileName}.csv", index=False)
