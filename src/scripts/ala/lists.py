from pathlib import Path
import lib.downloading as dl
import lib.bigFiles as bf
import lib.common as cmn

relevantLists = {
    "ARGA Threatened Species": "dr23195",
    "ARGA Useful Species": "dr23194",
    "ARGA Venomous and Poisonous Species": "dr23195",
    "ARGA Migratory Species": "dr23193",
    "ARGA Native Species": "dr23205",
    "ARGA Milestone Species": "dr23177",
    "ARGA Edible Species": "dr23094",
    "ARGA Exotic Species": "dr23197",
    "ARGA Bushfire Recovery": "dr25948",
    "ARGA Commercial Species": "dr23169",
    "ARGA Crop Wild Relatives": "dr23173",
}

def collect(outputPath: Path) -> None:
    subDir = outputPath.parent / "sections"
    subDir.mkdir()

    for listName, dataResourceUID in relevantLists.items():
        dl.download(f"https://lists-ws.test.ala.org.au/v2/download/{dataResourceUID}?zip=false", subDir / f"{listName.replace(' ', '_')}.csv", verbose=True)
    
    bf.combineDirectoryFiles(outputPath, subDir)
    cmn.clearFolder(subDir, True)
