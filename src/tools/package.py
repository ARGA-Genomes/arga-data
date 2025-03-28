from lib.data.argParser import ArgParser
from lib.processing.stages import Step

if __name__ == '__main__':
    parser = ArgParser(description="Package converted data")

    sources, overwrite, verbose, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.package()
