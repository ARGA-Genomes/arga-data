from lib.data.argParser import ArgParser
import logging

if __name__ == '__main__':
    parser = ArgParser(description="Run update on data source")
    parser.add_argument("-f", "--force", action="store_true", help="Force update regardless of config")
    
    sources, flags, args = parser.parse_args()
    kwargs = parser.namespaceKwargs(args)
    for source in sources:
        if not source.checkUpdateReady() and not args.force:
            logging.info(f"Data source '{source}' is not ready for update.")
            continue

        source.update(flags)
        outputFile = source.package()
