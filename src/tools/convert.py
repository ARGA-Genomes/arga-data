from lib.data.argParser import ArgParser
from lib.processing.stages import Step

if __name__ == '__main__':
    parser = ArgParser(description="Convert preDWC file to DWC")
    parser.add_argument("-i", "--ignoreRemapErrors", action="store_true", help="Ignore remapping errors from matching columns")
    parser.add_argument("-f", "--forceRetrieve", action="store_true", help="Force retrieve maps from google sheets")

    sources, flags, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.create(Step.CONVERSION, flags, **kwargs)
