import pandas as pd
from lib.data.argParser import ArgParser
import random
import logging
import lib.dataframes as dff
from typing import Generator
from lib.processing.files import DataFormat

def _collectFields(iterator: Generator[pd.DataFrame, None, None], entryLimit: int, seed: int) -> dict[str, pd.Series]:

    def columnCleanup(series: pd.Series) -> pd.Series:
        shortSeries = series.dropna()
        if len(shortSeries) > entryLimit:
            return shortSeries.sample(n=entryLimit, random_state=seed)
        
        return shortSeries.add(["" * (entryLimit - len(shortSeries))])

    df = next(iterator)
    for idx, chunk in enumerate(iterator, start=1):
        print(f"Scanning chunk: {idx}", end='\r')
        df = pd.concat([df, chunk], ignore_index=True)
        df = df.drop_duplicates()
        df = df.apply(columnCleanup, axis=0)

    return df

def _collectRecords(iterator: Generator[pd.DataFrame, None, None], entryLimit: int, seed: int) -> dict[str, pd.Series]:
    nanColumn = "NaN"
    df = next(iterator)
    for idx, chunk in enumerate(iterator, start=1):
        print(f"Scanning chunk: {idx}", end='\r')
        df = pd.concat([df, chunk], ignore_index=True)
        df = df.drop_duplicates()

        if len(df) > entryLimit:
            df = df.sample(n=entryLimit, random_state=seed)

        df.reset_index()
        df[nanColumn] = df.isna().sum(axis=1).sort_values(ascending=True)
        df = df.sort_values(nanColumn, axis=0, ignore_index=True)
        df = df.drop([nanColumn], axis=1)
        df = df.head(entryLimit)

    return df

if __name__ == '__main__':
    nameMap = {
        "d": "downloaded",
        "p": "processed",
        "c": "converted"
    }

    parser = ArgParser(description="Get column examples of pre-Conversion files")
    parser.addArgument("step", type=str, default="p", choices=list(nameMap), nargs="?", help="Config step to target")
    parser.addArgument("task", type=int, default=-1, nargs="?", help="Task within config step to sample")

    parser.addArgument('-e', '--entries', type=int, default=50, help="Number of unique entries to get")
    parser.addArgument('-i', '--ignoreRecord', action="store_true", help="Ignore records, searching for unique values")
    parser.addArgument('-c', '--chunksize', type=int, default=1024, help="File chunk size to read at a time")
    parser.addArgument('-d', '--seed', type=int, default=-1, help="Specify seed to run")
    parser.addArgument('-f', '--firstrow', type=int, default=0, help="First row offset for reading data")
    parser.addArgument('-r', '--rows', type=int, help="Maximum amount of rows to read from file")
    parser.addArgument('-t', '--tsv', action="store_true", help="Output as a tsv instead of csv")

    sources, flags, args = parser.parseArgs()
    entryLimit = args.entries

    for source in sources:
        dataFiles = source.getStepOutputs(args.step, args.task)
        if not dataFiles:
            continue

        outputDir = source.exampleDir

        if not outputDir.exists():
            outputDir.mkdir()

        seed = args.seed if args.seed >= 0 else random.randrange(2**32 - 1) # Max value for pandas seed
        random.seed(seed)

        for idx, file in enumerate(dataFiles, start=1):
            if file.format not in (DataFormat.CSV, DataFormat.TSV, DataFormat.PARQUET):
                logging.warning(f"Datafile #{idx} is not a valid format for sampling.")
                continue

            outputName = f"{source.name}_" # Source name
            outputName += nameMap.get(args.step) # Source step
            outputName += f"_{args.task}_{idx}_" # Source task and task file numbers
            outputName += "fields" if args.ignoreRecord else "records" # Data sample type
            outputName += f"_{args.chunksize}_{seed}" # Paramter info
            outputName += ".tsv" if args.tsv else ".csv" # Suffix
            outputPath = outputDir / outputName

            dfIterator = file.readIterator(args.chunksize, on_bad_lines="skip", low_memory=False)
            df = _collectFields(dfIterator, args.entries, seed) if args.ignoreRecord else _collectRecords(dfIterator, args.entries, seed)

            df = dff.removeSpaces(df)
            df.index += 1 # Increment index so output is 1-indexed numbers

            unknownColumn = "Unnamed: 0"
            if unknownColumn in df.columns:
                df = df.drop([unknownColumn], axis=1)

            df.to_csv(outputPath, sep="\t" if args.tsv else ",", index_label="Example #")
            logging.info(f"Created file {outputPath}")
