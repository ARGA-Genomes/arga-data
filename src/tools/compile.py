from lib.data.argParser import ArgParser
from lib.processing.files import Step

if __name__ == '__main__':
    parser = ArgParser(
        description="Compile data"
    )

    sources, flags, args = parser.parseArgs()
    for source in sources:
        source.create(Step.COMPILING, flags)
