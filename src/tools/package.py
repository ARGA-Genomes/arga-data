from lib.data.argParser import ArgParser
from lib.processing.files import Step

if __name__ == '__main__':
    parser = ArgParser(description="Package converted data")

    sources, args, prepArgs, execArgs = parser.parseArgs()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.package()
