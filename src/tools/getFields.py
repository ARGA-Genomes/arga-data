import pandas as pd
import json
from lib.data.argParser import ArgParser
from lib.processing.stages import File, Step
import random
import logging
import lib.dataframes as dff

def _collectFields(stageFile: File, entryLimit: int, chunkSize: int, seed: int, offset: int = 0, rows: int = None) -> dict[str, pd.Series]:
    for idx, chunk in enumerate(stageFile.loadDataFrameIterator(chunkSize, offset, rows), start=1):
        print(f"Scanning chunk: {idx}", end='\r')
        sample = chunk.apply(lambda x: x.dropna().drop_duplicates())

        if idx == 1:
            df = sample
            continue

        df = pd.concat([df, sample], ignore_index=True)
        df = df.apply(lambda x: x.drop_duplicates().sample(n=entryLimit, replace=True, random_state=seed).drop_duplicates())

    return {column: df[column].dropna().tolist() for column in df.columns}

def _collectRecords(stageFile: File, entryLimit: int, chunkSize: int, seed: int, offset: int = 0, rows: int = None) -> dict[str, pd.Series]:
    for idx, chunk in enumerate(stageFile.loadDataFrameIterator(chunkSize, offset, rows), start=1):
        print(f"Scanning chunk: {idx}", end='\r')
        sample = chunk.sample(n=min(len(chunk), entryLimit), random_state=seed)

        if idx == 1:
            df = sample
            continue

        df = pd.concat([df, sample])
        emptyDF = df.isna().sum(axis=1)
        indexes = [idx for idx, _ in sorted(emptyDF.items(), key=lambda x: x[1])]
        df = df.loc[indexes[:entryLimit]]

    return {column: df[column].tolist() for column in df.columns}

if __name__ == '__main__':
    parser = ArgParser(description="Get column names of preDwc files")
    parser.add_argument('-e', '--entries', type=int, default=50, help="Number of unique entries to get")
    parser.add_argument('-t', '--tsv', action="store_true", help="Output as tsv instead")
    parser.add_argument('-u', '--uniques', action="store_true", help="Find unique values only, ignoring record")
    parser.add_argument('-c', '--chunksize', type=int, default=128, help="File chunk size to read at a time")
    parser.add_argument('-s', '--seed', type=int, default=-1, help="Specify seed to run")
    parser.add_argument('-f', '--firstrow', type=int, default=0, help="First row offset for reading data")
    parser.add_argument('-r', '--rows', type=int, help="Maximum amount of rows to read from file")

    sources, overwrite, verbose, args = parser.parse_args()
    entryLimit = args.entries

    for source in sources:
        outputDir = source.databaseDir / "examples"
        if not outputDir.exists():
            outputDir.mkdir()

        extension = "tsv" if args.tsv else "json"
        source._prepare(Step.CONVERSION)
        stageFile = source.processingManager.getLatestNodeFile()[0] # Should be singular stage file before DwC

        if not stageFile.filePath.exists():
            print(f"File {stageFile.filePath} does not exist, have you run preDwCCreate.py yet?")
            continue

        seed = args.seed if args.seed >= 0 else random.randrange(2**32 - 1) # Max value for pandas seed
        random.seed(seed)


        columns = stageFile.getColumns()
        mappingSuccess = source.conversionManager.remapper.buildTable(columns)
        if not mappingSuccess:
            logging.warning("Unable to build translation table, output will not contain mappings")

        valueType = "fields" if args.uniques else "records"
        logging.info(f"Collecting {valueType}...")

        if args.uniques:
            values = _collectFields(stageFile, args.entries, args.chunksize, seed, args.firstrow, args.rows)
        else:
            values = _collectRecords(stageFile, args.entries, args.chunksize, seed, args.firstrow, args.rows)

        output = outputDir / f"{valueType}_{args.chunksize}_{seed}.{extension}"

        if mappingSuccess:
            data = {column: {"Maps to": [{"Event": mappedColumn.event.value, "Column": mappedColumn.colName} for mappedColumn in source.conversionManager.remapper.table.getTranslation(column)], "Values": values[column]} for column in columns}
        else:
            data = {column: {"Maps to": "N/A", "Values": values[column]} for column in columns}

        logging.info(f"Writing to file {output}")
        if args.tsv:
            dfData = {k: v["Values"] + ["" for _ in range(entryLimit - len(v["Values"]))] for k, v in data.items()}
            df = pd.DataFrame.from_dict(dfData)
            df = dff.removeSpaces(df)
            df.index += 1 # Increment index so output is 1-indexed numbers
            df.to_csv(output, sep="\t", index_label="Example #")
        else:
            with open(output, 'w') as fp:
                json.dump(data, fp, indent=4)
