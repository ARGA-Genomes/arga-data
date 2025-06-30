from lib.data.argParser import ArgParser
from lib.processing.files import Step

if __name__ == '__main__':
    parser = ArgParser(
        description="Download source data",
        reprepareHelp="Force retrieval of download information"
    )

    sources, flags, kwargs = parser.parseArgs()
    for source in sources:
        source.create(Step.DOWNLOAD, flags)
