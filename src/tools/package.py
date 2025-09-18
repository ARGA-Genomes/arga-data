from lib.data.argParser import ArgParser

if __name__ == '__main__':
    parser = ArgParser(description="Package converted data")

    sources, flags, args = parser.parseArgs()
    for source in sources:
        source.package()
