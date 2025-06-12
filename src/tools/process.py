from lib.data.argParser import ArgParser
from lib.processing.stages import Step

if __name__ == '__main__':
    parser = ArgParser(description="Prepare for DwC conversion")
    
    sources, flags, kwargs = parser.parseArgs()
    for source in sources:
        source.create(Step.PROCESSING, flags)
