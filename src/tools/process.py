from lib.data.argParser import ArgParser
from lib.processing.files import Step

if __name__ == '__main__':
    parser = ArgParser(description="Prepare for DwC conversion")
    
    sources, flags, args = parser.parseArgs()
    for source in sources:
        source.create(Step.PROCESSING, flags)
