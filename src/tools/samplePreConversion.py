import pandas as pd
from lib.data.argParser import ArgParser
from lib.processing.stages import File, Step
import random
import logging
import lib.dataframes as dff
from typing import Generator

def _collectFields(iterator: Generator[pd.DataFrame, None, None], entryLimit: int, seed: int) -> dict[str, pd.Series]:
    df = next(iterator).fillna("").drop_duplicates()
    for idx, chunk in enumerate(iterator, start=1):
        print(f"Scanning chunk: {idx}", end='\r')
        chunk = chunk.fillna("").drop_duplicates()
        df = pd.concat([df, chunk], ignore_index=True)
        df.drop_duplicates().sample(n=entryLimit, replace=True, random_state=seed)

    return df

def _collectRecords(iterator: Generator[pd.DataFrame, None, None], entryLimit: int, seed: int) -> dict[str, pd.Series]:
    df = next(iterator).sample(n=min(len(df), entryLimit), random_state=seed)
    for idx, chunk in enumerate(iterator, start=1):
        print(f"Scanning chunk: {idx}", end='\r')
        chunk = chunk.sample(n=min(len(df), entryLimit), random_state=seed)
        df = pd.concat([df, chunk])
        emptyDF = df.isna().sum(axis=1)
        indexes = [idx for idx, _ in sorted(emptyDF.items(), key=lambda x: x[1])]
        df = df.loc[indexes[:entryLimit]]

    return df

if __name__ == '__main__':
    parser = ArgParser(description="Get column names of pre-Conversion files")
    parser.add_argument('-e', '--entries', type=int, default=50, help="Number of unique entries to get")
    parser.add_argument('-i', '--ignoreRecord', action="store_true", help="Ignore records, searching for unique values")
    parser.add_argument('-c', '--chunksize', type=int, default=1024, help="File chunk size to read at a time")
    parser.add_argument('-s', '--seed', type=int, default=-1, help="Specify seed to run")
    parser.add_argument('-f', '--firstrow', type=int, default=0, help="First row offset for reading data")
    parser.add_argument('-r', '--rows', type=int, help="Maximum amount of rows to read from file")

    sources, flags, kwargs = parser.parseArgs()
    entryLimit = kwargs.entries

    for source in sources:
        outputDir = source.exampleDir
        if not outputDir.exists():
            outputDir.mkdir()

        source._prepare(Step.CONVERSION, flags)
        stageFile = source.processingManager.getLatestNodeFile() # Should be singular stage file before DwC

        if not stageFile.filePath.exists():
            print(f"File {stageFile.filePath} does not exist, please run all required downloading/processing.")
            continue

        seed = kwargs.seed if kwargs.seed >= 0 else random.randrange(2**32 - 1) # Max value for pandas seed
        random.seed(seed)
        outputPath = outputDir / f"{'fields' if kwargs.ignoreRecord else 'records'}_{kwargs.chunksize}_{seed}.tsv"

        dfIterator = stageFile.loadDataFrameIterator(kwargs.chunksize, kwargs.firstrow, kwargs.rows)
        df = _collectFields(dfIterator, kwargs.entries, seed) if kwargs.ignoreRecord else _collectRecords(dfIterator, kwargs.entries, seed)

        df = dff.removeSpaces(df)
        df.index += 1 # Increment index so output is 1-indexed numbers
        df.to_csv(outputPath, sep="\t", index_label="Example #")
        logging.info(f"Created file {outputPath}")
