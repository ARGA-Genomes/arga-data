from lib.data.argParser import ArgParser
from lib.processing.stages import Step

if __name__ == '__main__':
    parser = ArgParser(description="Prepare for DwC conversion")
    
    sources, flags, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        source.create(Step.PROCESSING, flags, **kwargs)
