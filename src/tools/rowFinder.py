from lib.data.argParser import ArgParser
from lib.processing.files import Step
import pandas as pd

if __name__ == '__main__':
    parser = ArgParser(description="Get column examples of pre-Conversion files")
    parser.addArgument("column", type=str, help="Column name to search")
    parser.addArgument("values", type=str, nargs="*", help="Value to check for in column")
    parser.addArgument('-c', '--chunksize', type=int, default=1024, help="File chunk size to read at a time")

    sources, flags, args = parser.parseArgs()

    for source in sources:
        source._prepare(Step.PROCESSING, flags)
        stageFile = source.processingManager.getLatestNodeFile() # Should be singular stage file before DwC

        if not stageFile.exists():
            print(f"File {stageFile.path} does not exist, please run all required downloading/processing.")
            continue

        if args.column not in stageFile.getColumns():
            print(f"Column '{args.column}' does not exists in file")
            continue

        foundRows = []
        for idx, df in enumerate(stageFile.readIterator(args.chunksize, on_bad_lines="skip"), start=1):
            print(f"At chunk: {idx}", end="\r")

            df = df[df[args.column].isin(args.values)]
            if df.empty:
                continue

            foundRows.append(df)

        if not foundRows:
            print("\nNo rows found")
            continue

        print()
        print(pd.concat(foundRows))
