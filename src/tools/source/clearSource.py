from lib.data.argParser import SourceArgParser
import lib.commonFuncs as cmn
from lib.tools.logger import Logger

if __name__ == '__main__':
    parser = SourceArgParser(description="Clean up source to save space")
    parser.add_argument("-r", "--raw", action="store_true", help="Clear raw/downloaded files too")

    sources, _, _, args = parser.parse_args()
    for source in sources:
        dataDir = source.getBaseDir() / "data"

        for folder in dataDir.iterdir():
            if folder.is_file(): # Skip files that may be in dir
                continue

            if folder.name == "raw" and not args.raw: # Only delete raw folder contents if necessary
                continue

            if folder.name == "dwc": # Clear non-zip files in dwc folder
                Logger.info(f"Clearing folder: dwc")
                for item in folder.iterdir():
                    if item.suffix == ".zip":
                        continue

                    if item.is_file():
                        item.unlink()
                    else:
                        cmn.clearFolder(item, True)

                continue

            Logger.info(f"Clearing folder: {folder.name}")
            cmn.clearFolder(folder)
