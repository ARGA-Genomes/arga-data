from lib.data.argParser import ArgParser
from lib.processing.stages import Step

if __name__ == '__main__':
    parser = ArgParser(description="Download source data")

    sources, flags, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.create(Step.DOWNLOAD, flags, **kwargs)
