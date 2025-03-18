import argparse
from lib.data.sources import SourceManager
from lib.data.database import BasicDB, Flag

class ArgParser:
    def __init__(self, description=""):
        self.sourceWarning = 4
        self.parser = argparse.ArgumentParser(description=description)
        self.manager = SourceManager()

        self.parser.add_argument("source", help="Data set to interact with", metavar="SOURCE")
        self.parser.add_argument("-p", "--prepare", action="store_true", help="Force redoing preparation")
        self.parser.add_argument("-o", "--overwrite", action="store_true", help="Force overwriting output")
        self.parser.add_argument("-u", "--update", action="store_true", help="Run update version of database if available")
        self.parser.add_argument("-q", "--quiet", action="store_false", help="Suppress output")

    def add_argument(self, *args, **kwargs) -> None:
        self.parser.add_argument(*args, **kwargs)

    def parse_args(self, *args, **kwargs) -> tuple[list[BasicDB], list[Flag], argparse.Namespace]:
        parsedArgs = self.parser.parse_args(*args, **kwargs)

        sources = self.manager.requestDBs(self._extract(parsedArgs, "source"))
        if len(sources) >= self.sourceWarning:
            passed = self._warnSources(len(sources))
            if not passed:
                sources = []

        flagMap = {
            "quiet": Flag.VERBOSE,
            "prepare": Flag.PREPARE_OVERWRITE,
            "overwrite": Flag.OUTPUT_OVERWRITE,
            "update": Flag.UPDATE
        }

        flags = [flag for key, flag in flagMap.items() if self._extract(parsedArgs, key)]

        return sources, flags, parsedArgs
    
    def namespaceKwargs(self, namespace: argparse.Namespace) -> dict:
        return vars(namespace)

    def add_mutually_exclusive_group(self, *args, **kwargs) -> argparse._MutuallyExclusiveGroup:
        return self.parser.add_mutually_exclusive_group(*args, **kwargs)
    
    def _extract(self, namespace: argparse.Namespace, attribute: str) -> any:
        attr = getattr(namespace, attribute)
        delattr(namespace, attribute)
        return attr

    def _warnSources(self, sourceCount: int) -> bool:
        response = input(f"WARNING: Attempting to do work on {sourceCount} sources, which may take a long time, are you sure you want to coninue? (y/n): ")
        while response.lower() not in ("y", "n"):
            response = input(f"Invalid response '{response}', continue? (y/n): ")

        return response.lower() == "y"
