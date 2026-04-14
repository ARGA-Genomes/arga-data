from lib.data.argParser import ArgParser
import logging

if __name__ == '__main__':
    parser = ArgParser(description="Run update on data source")
    parser.addArgument("-f", "--force", action="store_true", help="Force update regardless of config")
    
    sources, flags, args = parser.parseArgs()
    for source in sources:
        source.update()
