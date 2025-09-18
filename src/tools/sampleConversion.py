from lib.data.argParser import ArgParser
from lib.processing.files import Step
from lib.processing.mapping import Map
from lib.processing.files import StackedFile
import logging

if __name__ == '__main__':
    parser = ArgParser(description="View portion of Converted file")
    parser.addArgument("-e", "--entries", type=int, default=100, help="Amount of entries to view")
    parser.addArgument("-t", "--tsv", action="store_true", help="Output file as TSV instead")
    columnGroup = parser.addMutuallyExclusiveGroup()
    columnGroup.add_argument("-m", "--mapped", action="store_true", help="Get only mapped fields")
    columnGroup.add_argument("-U", "--unmapped", action="store_true", help="Get only unmapped fields")
    
    sources, flags, args = parser.parseArgs()
    suffix = ".tsv" if args.tsv else ".csv"
    delim = "\t" if args.tsv else ","

    for source in sources:
        source._prepare(Step.CONVERSION, flags)
        lastConversionFile = source.conversionManager.getLastOutput()
        if lastConversionFile is None:
            continue

        if not lastConversionFile.exists():
            continue

        outputFolder = lastConversionFile.name
        if args.mapped:
            outputFolder += "_mapped"
        elif args.unmapped:
            outputFolder += "_unmapped"
        outputFolder += "_example"

        stackedFile = StackedFile(lastConversionFile)
        df = next(stackedFile.loadDataFrameIterator(rows=args.entries))

        folderPath = source.exampleDir / outputFolder
        folderPath.mkdir(exist_ok=True)

        dummpyMap = Map({})

        for event in df.columns.levels[0]:
            if (args.mapped and event == dummpyMap._unmappedLabel) or (args.unmapped and event != dummpyMap._unmappedLabel):
                continue

            fileName = f"{event}{suffix}"
            df[event].to_csv(folderPath / fileName, sep=delim, index=False)

        logging.info(f"Created folder: {folderPath}")
