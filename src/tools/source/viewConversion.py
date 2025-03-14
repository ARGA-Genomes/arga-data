from lib.data.argParser import ArgParser
from lib.processing.stages import Step
from lib.processing.mapping import Event
from lib.tools.logger import Logger

if __name__ == '__main__':
    parser = ArgParser(description="View portion of DWC file")
    parser.add_argument("-e", "--entries", type=int, default=100, help="Amount of entries to view")
    parser.add_argument("-t", "--tsv", action="store_true", help="Output file as TSV instead")
    columnGroup = parser.add_mutually_exclusive_group()
    columnGroup.add_argument("-m", "--mapped", action="store_true", help="Get only mapped fields")
    columnGroup.add_argument("-u", "--unmapped", action="store_true", help="Get only unmapped fields")
    
    sources, selectedFiles, overwrite, args = parser.parse_args()
    suffix = ".tsv" if args.tsv else ".csv"
    delim = "\t" if args.tsv else ","

    for source in sources:
        source._prepare(Step.CONVERSION)
        dwcFile = source.conversionManager.output

        if not dwcFile.filePath.exists():
            print(f"DwC file {dwcFile.filePath} does not exist, have you run convert.py?")
            continue

        df = next(dwcFile.loadDataFrameIterator(args.entries))
        folderName = dwcFile.filePath.name
        if args.mapped:
            folderName += "_mapped"
        elif args.unmapped:
            folderName += "_unmapped"
        folderName += "_example"

        folderPath = source.getBaseDir() / "examples" / folderName
        folderPath.mkdir(exist_ok=True)

        for event in df.columns.levels[0]:
            if (args.mapped and event == Event.UNMAPPED.value) or (args.unmapped and event != Event.UNMAPPED.value):
                continue

            fileName = f"{event}{suffix}"
            df[event].to_csv(folderPath / fileName, sep=delim, index=False)

        Logger.info(f"Created folder: {folderPath}")
