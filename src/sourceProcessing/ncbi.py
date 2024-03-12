import pandas as pd
import lib.commonFuncs as cmn
import lib.dataframeFuncs as dff
from pathlib import Path
from lib.tools.bigFileWriter import BigFileWriter
import concurrent.futures
import sourceProcessing.ncbiFlatfileParser as ffp
from lib.tools.extractor import Extractor
from io import StringIO
import json

def splitLine(line: str, endingDivider: bool = True) -> list[str]:
    cleanLine = line.rstrip('\n').rstrip()
    if endingDivider:
        cleanLine = cleanLine.rstrip('|')
    return [element.strip() for element in cleanLine.split('|')]

def compileBiocollections(fileDir: Path, outputFilePath: Path) -> None:
    collCodes = fileDir / "Collection_codes.txt"
    instCodes = fileDir /"Institution_codes.txt"
    uInstCodes = fileDir / "Unique_institution_codes.txt"

    for ref, filePath in enumerate((collCodes, instCodes, uInstCodes)):
        data = []

        with open(filePath, encoding='utf-8') as fp:
            line = fp.readline()
            headers = splitLine(line)
            line = fp.readline() # Blank line 2 in every file, call again
            line = fp.readline()
            while line:
                data.append(splitLine(line, True))
                line = fp.readline()

        # cull extra data that doesn't map to a header
        df = pd.DataFrame([line[:len(headers)] for line in data], columns=headers) 

        if ref == 0:
            output = df.copy()
        else:
            output = pd.merge(output, df, 'left')

    output.dropna(how='all', axis=1, inplace=True)
    output.to_csv(outputFilePath, index=False)

def augmentBiosample(df: pd.DataFrame) -> pd.DataFrame:
    return dff.splitField(df, "ncbi_lat long", cmn.latlongToDecimal, ["decimalLatitude", "decimalLongitude"])

def parseStats(filePath: Path) -> dict:
    with open(filePath, encoding="utf-8") as fp:
        data = fp.read()

    splitData = data.rsplit("#", 1)
    if len(splitData) != 2: # No # found, problem with file
        return {}
    
    info, table = splitData

    # Check that file has actual usable data
    firstLine, info = info.split("\n", 1)
    if firstLine != "# Assembly Statistics Report":
        return {}

    # Parsing info
    data = {}
    for line in info.split("\n"):
        if ":" not in line: # Done reading information
            break
        
        key, value = line.split(":", 1)
        data[key.strip("# ")] = value.strip()

    # Parsing table
    df = pd.read_csv(StringIO(table.lstrip()), sep="\t")
    df = df[(df["unit-name"] == "all") & (df["molecule-name"] == "all") & (df["molecule-type/loc"] == "all") & (df["sequence-type"] == "all")]

    data |= dict(zip(df["statistic"], df["value"])) # Adding table data

    return data

def compileAssemblyStats(inputFolder: Path, outputFilePath: Path) -> None:
    writer = BigFileWriter(outputFilePath, "assemblySections", "section")

    records = []
    # for idx, file in enumerate(inputFolder.iterdir(), start=1):
    #     print(f"At entry: {idx}", end="\r")

    #     result = parseStats(file)
    #     if not result:
    #         continue

    #     records.append(result)

    #     if len(records) >= 100000:
    #         writer.writeDF(pd.DataFrame.from_records(records))
    #         records.clear()
    with concurrent.futures.ProcessPoolExecutor(max_workers=20) as executor:
        futures = (executor.submit(parseStats, filePath) for filePath in inputFolder.iterdir())
        for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            print(f"At entry: {idx}", end="\r")

            result = future.result()
            if not result:
                continue

            records.append(future.result())

            if len(records) >= 100000:
                writer.writeDF(pd.DataFrame.from_records(records))
                records.clear()

    print()
    writer.oneFile()

def parseNucleotide(rawFilesPath: Path, outputFilePath: Path, verbose: bool = False) -> None:
    writer = BigFileWriter(outputFilePath, "seqChunks", "chunk")
    extractor = Extractor(outputFilePath.parent, overwrite=True)

    for idx, file in enumerate(rawFilesPath.iterdir(), start=1):
        if verbose:
            print(f"Extracting file {file.name}")
        else:
            print(f"Processing file: {idx}", end="\r")
    
        extractedFile = extractor.run(file)

        if extractedFile is None:
            print(f"Failed to extract file {file.name}, skipping")
            continue

        if verbose:
            print(f"Parsing file {extractedFile}")

        df = ffp.parseFlatfile(extractedFile, verbose)
        writer.writeDF(df)

        extractedFile.unlink()

    writer.oneFile()
