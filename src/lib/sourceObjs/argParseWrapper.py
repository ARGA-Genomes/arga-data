import argparse
from lib.sourceManager import SourceManager
from lib.sourceObjs.sourceDatabase import Database

class SourceArgParser:
    def __init__(self, description=""):
        self.parser = argparse.ArgumentParser(description=description)
        self.manager = SourceManager()

        self.parser.add_argument("sources", choices=self.manager.choices(), nargs="+", help="Database to interact with", metavar="SOURCE")
        self.parser.add_argument("-o", "--overwrite", type=int, const=10, nargs="?", default=0, help="Amount of steps to force overwrite on")
        
        fileSelection = self.parser.add_subparsers(help="File selection")

        specificParser = fileSelection.add_parser("specific")
        specificParser.add_argument("-n", "--filenums", type=int, required=True, nargs="+", default=[], help="Choose which file number to interact with")

        rangeParser = fileSelection.add_parser("range")
        rangeParser.add_argument("-f", "--firstfile", type=int, required=True, default=0, help="First file to start select range from")
        rangeParser.add_argument("-q", "--quantity", type=int, required=True, default=1, help="Quantity of files to select")

    def add_argument(self, *args, **kwargs) -> None:
        self.parser.add_argument(*args, **kwargs)

    def parse_args(self, *args, **kwargs) -> tuple[list[Database], list[int], int, dict]:
        parsedArgs = self.parser.parse_args(*args, **kwargs)
        sources = self.manager.getMultipleDBs(parsedArgs.sources)
        overwrite = parsedArgs.overwrite

        delattrs = ["sources", "overwrite"] # Remove source name stored in original parsed args

        selectedFiles = []
        if hasattr(parsedArgs, "filenums"): # Using specific selection
            selectedFiles = parsedArgs.filenums
            delattrs.append("filenums")

        elif hasattr(parsedArgs, "firstfile"): # Using range selection
            selectedFiles = list(range(parsedArgs.firstfile, parsedArgs.quantity))
            delattrs.extend(["firstfile", "quantity"])

        for attr in delattrs:
            delattr(parsedArgs, attr)

        return (sources, selectedFiles, overwrite, vars(parsedArgs))

    def add_mutually_exclusive_group(self, *args, **kwargs) -> argparse._MutuallyExclusiveGroup:
        self.parser.add_mutually_exclusive_group(*args, **kwargs)
