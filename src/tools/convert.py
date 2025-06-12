from lib.data.argParser import ArgParser
from lib.processing.stages import Step

if __name__ == '__main__':
    parser = ArgParser(
        description="Convert file to ARGA schema",
        reprepareHelp="Force retrieval of map",
    )

    sources, flags, kwargs = parser.parseArgs()
    for source in sources:
        source.create(Step.CONVERSION, flags)
