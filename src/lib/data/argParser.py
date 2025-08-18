from argparse import ArgumentParser, Namespace, _MutuallyExclusiveGroup
from lib.data.sources import SourceManager
from lib.data.database import BasicDB, Flag

class ArgParser:
    def __init__(self, description: str = "", reprepareHelp: str = "Force redoing source preparation", overwriteHelp: str = "Force overwriting all output"):
        self.sourceWarning = 4

        self._parser = ArgumentParser(description=description)
        self._manager = SourceManager()

        self.addArgument("source", help="Data set to interact with", metavar="SOURCE")
        self.addArgument("-q", f"--{Flag.VERBOSE.value}", action="store_false", help="Suppress output during execution")

        self.addArgument("-p", f"--{Flag.REPREPARE.value}", action="store_true", help=reprepareHelp)
        self.addArgument("-o", f"--{Flag.OVERWRITE.value}", action="store_true", help=overwriteHelp)

    def addArgument(self, *args, **kwargs) -> None:
        self._parser.add_argument(*args, **kwargs)

    def parseArgs(self, *args, kwargsDict: bool = False, **kwargs) -> tuple[list[BasicDB], list[Flag], Namespace | dict]:
        parsedArgs = self._parser.parse_args(*args, **kwargs)

        sources = self._manager.matchSources(self._extract(parsedArgs, "source"))
        sourceCount = self._manager.countSources(sources)
        if sourceCount >= self.sourceWarning:
            passed = self._warnSources(sourceCount)
            if not passed:
                sources = {}

        flags = [flag for key, flag in Flag._value2member_map_.items() if self._extract(parsedArgs, key)]
        constructedSources = self._manager.constructDBs(sources)

        return constructedSources, flags, parsedArgs.__dict__ if kwargsDict else parsedArgs

    def addMutuallyExclusiveGroup(self, *args, **kwargs) -> _MutuallyExclusiveGroup:
        return self._parser.add_mutually_exclusive_group(*args, **kwargs)
    
    def _extract(self, namespace: Namespace, attribute: str) -> any:
        attr = getattr(namespace, attribute)
        delattr(namespace, attribute)
        return attr

    def _warnSources(self, sourceCount: int) -> bool:
        response = input(f"WARNING: Attempting to do work on {sourceCount} sources, which may take a long time, are you sure you want to coninue? (y/n): ")
        while response.lower() not in ("y", "n"):
            response = input(f"Invalid response '{response}', continue? (y/n): ")

        return response.lower() == "y"
