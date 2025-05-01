from pathlib import Path
from lib.bigFileWriter import BigFileWriter
import lib.zipping as zip
import pandas as pd
from lib.progressBar import SteppableProgressBar

def _parseMetadata(line: str) -> tuple[str, dict]:
    key, data = line[2:].split("=", 1)
    if "=" not in data:
        return key, {}
    
    data = data.strip("<>")
    pairs = []

    while len(data):
        commaPos = data.find(",")
        quotePos = data.find("\"")

        if quotePos >= 0 and quotePos < commaPos:
            commaPos = data.find(",", data.find("\"", quotePos+1))

        value = data[:commaPos] if commaPos >= 0 else data
        data = data[commaPos+1:] if commaPos >= 0 else ""

        pairs.append(value.replace('"', "").split("=", 1))

    return key, {k: v for k, v in pairs}

def _parseInfoRow(item: str) -> pd.Series:
    if not isinstance(item, str):
        return pd.Series(dtype=object)
    
    values = item.split(";")
    splitValues = [v.split("=", 1) for v in values]
    data = {}
    for value in splitValues:
        if len(value) == 1:
            data[value[0]] = True
            continue

        key, value = value
        if key == "SID":
            value = value.split(",")

        data[key] = value
    
    return pd.Series(data)

def _parseVCF(filePath: Path, outputName: str, writer: BigFileWriter) -> Path:
    metadata = {}
    with open(filePath) as fp:
        line = fp.readline().rstrip("\n")
        headerLine = 0
        while line.startswith("##"):
            key, fields = _parseMetadata(line)
            key = key.lower()

            if key not in metadata:
                metadata[key] = []
                
            metadata[key].append(fields)

            headerLine += 1
            line = fp.readline().rstrip("\n")

    contig = pd.DataFrame.from_records(metadata.get("contig", []))

    writtenFilesLen = len(writer.writtenFiles)
    iterator = pd.read_csv(filePath, sep="\t", header=headerLine, chunksize=500000, skip_blank_lines=True, dtype=object)
    for idx, df in enumerate(iterator):
        chunkName = f"{outputName}_{idx}"
        if chunkName in writer.getSubfileNames():
            continue

        df: pd.DataFrame = df.rename({column: column.strip("#").lower() for column in df.columns}, axis=1)
        if len(df) == 0:
            continue

        info = df["info"].apply(_parseInfoRow)
        for column in ("LOE", "RS_VALIDATED"):
            if column in info.columns:
                info[column].fillna(False)

        info = info.rename({
            "ALMM": "alleles_mismatch",
            "ASMM": "assembly_mismatch",
            "LOE": "lack_of_evidence",
            "REMAPPED": "remapped",
            "RS_VALIDATED": "rs_validated",
            "SID": "study_ids",
            "SS_VALIDATED": "ss_validated",
            "VC": "variant_class"
        }, axis=1)

        df = pd.concat([df, info], axis=1)
        df = df.merge(contig, "left", left_on="chrom", right_on="ID", copy=False)
        df = df.drop("ID", axis=1)

        for key, value in metadata["reference"][0].items():
            df[f"ref{key.capitalize()}"] = value

        writer.writeDF(df, chunkName)

    if writtenFilesLen == len(writer.writtenFiles):
        # No files written, likely due to empty vcf file
        return
    
    updateSubfile = writer.writtenFiles[writtenFilesLen]
    updateSubfile.rename(updateSubfile.filePath.parent / (updateSubfile.filePath.stem[:-2] + updateSubfile.fileFormat.value), updateSubfile.fileFormat)

def combine(inputDir: Path, outputFilePath: Path):
    writer = BigFileWriter(outputFilePath)
    writer.populateFromFolder()
    completedSubfiles = writer.getSubfileNames()
    progress = SteppableProgressBar(sum(1 for _ in inputDir.iterdir()))

    for file in inputDir.iterdir():
        trimmedName = file.name.rsplit(".", 2)[0].rsplit("_", 2)[0]

        if trimmedName not in completedSubfiles:
            extractedFile = zip.extract(file, outputFilePath.parent, verbose=False)
            _parseVCF(extractedFile, trimmedName, writer)
            extractedFile.unlink()

        progress.update()
    
    writer.oneFile()
