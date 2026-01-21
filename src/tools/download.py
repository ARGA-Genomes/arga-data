from lib.data.argParser import ArgParser, Step

if __name__ == '__main__':
    parser = ArgParser(
        description="Download source data",
        reprepareHelp="Force retrieval of download information"
    )

    sources, flags, args = parser.parseArgs()
    for source in sources:
        source.create(Step.DOWNLOADING, flags)
