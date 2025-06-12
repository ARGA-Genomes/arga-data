from lib.data.argParser import ArgParser
import lib.common as cmn
import logging

if __name__ == '__main__':
    parser = ArgParser(description="Clean up source to save space")
    parser.addArgument("-r", "--raw", action="store_true", help="Clear raw/downloaded files too")

    sources, flags, kwargs = parser.parseArgs()
    for source in sources:
        dataDir = source.baseDir / "data"

        for folder in dataDir.iterdir():
            if folder.is_file(): # Skip files that may be in dir
                continue

            if folder.name == "raw" and not args.raw: # Only delete raw folder contents if necessary
                continue

            if folder.name == "dwc": # Clear non-zip files in dwc folder
                logging.info(f"Clearing folder: dwc")
                for item in folder.iterdir():
                    if item.suffix == ".zip":
                        continue

                    if item.is_file():
                        item.unlink()
                    else:
                        cmn.clearFolder(item, True)

                continue

            logging.info(f"Clearing folder: {folder.name}")
            cmn.clearFolder(folder)
