import pandas as pd
from lib.data.argParser import ArgParser
from lib.processing.files import Step
import random
import logging
import lib.dataframes as dff
from typing import Generator

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
        df.reset_index()

    return df

if __name__ == '__main__':
    parser = ArgParser(description="Get column examples of pre-Conversion files")
    parser.addArgument('-e', '--entries', type=int, default=50, help="Number of unique entries to get")
    parser.addArgument('-i', '--ignoreRecord', action="store_true", help="Ignore records, searching for unique values")
    parser.addArgument('-c', '--chunksize', type=int, default=1024, help="File chunk size to read at a time")
    parser.addArgument('-s', '--seed', type=int, default=-1, help="Specify seed to run")
    parser.addArgument('-f', '--firstrow', type=int, default=0, help="First row offset for reading data")
    parser.addArgument('-r', '--rows', type=int, help="Maximum amount of rows to read from file")

    sources, flags, kwargs = parser.parseArgs()
    entryLimit = kwargs.entries

    for source in sources:
        outputDir = source.exampleDir
        if not outputDir.exists():
            outputDir.mkdir()

        source._prepare(Step.PROCESSING, flags)
        stageFile = source.processingManager.getLatestNodeFile() # Should be singular stage file before DwC

        if not stageFile.exists():
            print(f"File {stageFile.path} does not exist, please run all required downloading/processing.")
            continue

        seed = kwargs.seed if kwargs.seed >= 0 else random.randrange(2**32 - 1) # Max value for pandas seed
        random.seed(seed)
        outputPath = outputDir / f"{'fields' if kwargs.ignoreRecord else 'records'}_{kwargs.chunksize}_{seed}.tsv"

        dfIterator = stageFile.readIterator(kwargs.chunksize, on_bad_lines="skip")
        df = _collectFields(dfIterator, kwargs.entries, seed) if kwargs.ignoreRecord else _collectRecords(dfIterator, kwargs.entries, seed)

        df = dff.removeSpaces(df)
        df = df.reset_index()
        df.index += 1 # Increment index so output is 1-indexed numbers
        df.to_csv(outputPath, sep="\t", index_label="Example #")
        logging.info(f"Created file {outputPath}")
