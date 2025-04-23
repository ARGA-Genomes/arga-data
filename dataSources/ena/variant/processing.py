from pathlib import Path
from lib.bigFileWriter import BigFileWriter
import lib.zipping as zip
import pandas as pd
from lib.progressBar import SteppableProgressBar
from io import StringIO

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

        pairs.append(value.replace('"', "").split("="))

    return key, {k: v for k, v in pairs}

def _parseInfoRow(item: str) -> pd.Series:
    if not isinstance(item, str):
        return pd.Series(dtype=object)
    
    values = item.split(";")
    splitValues = [v.split("=", 1) for v in values]
    data = {"info": item}
    for value in splitValues:
        if len(value) == 1:
            data[value[0]] = True
            continue

        key, value = value
        if key == "SID":
            value = value.split(",")

        data[key] = value
    
    return pd.Series(data)

def parseVCF(filePath: Path) -> pd.DataFrame:
    metadata = {}
    with open(filePath) as fp:
        line = fp.readline().rstrip("\n")
        while line.startswith("##"):
            key, fields = _parseMetadata(line)
            key = key.lower()

            if key not in metadata:
                metadata[key] = []
                
            metadata[key].append(fields)

            line = fp.readline().rstrip("\n")

        columns = [item.strip("#").lower() for item in line.split("\t")]
        df = pd.read_csv(fp, sep="\t", names=columns)

    print(df.head(10))
    contig = pd.DataFrame.from_records(metadata["contig"])
    info = df["info"].apply(_parseInfoRow)
    for column in ("LOE", "RS_VALIDATED"):
        info[column].fillna(False)

    info.rename({
        "ALMM": "alleles_mismatch",
        "ASMM": "assembly_mismatch",
        "LOE": "lack_of_evidence",
        "REMAPPED": "remapped",
        "RS_VALIDATED": "rs_validated",
        "SID": "study_ids",
        "SS_VALIDATED": "ss_validated",
        "VC": "variant_class"
    })

    df = df.merge(info, "left", on="info", copy=False)
    df = df.merge(contig, "left", left_on="chrom", right_on="ID", copy=False)
    df = df.drop("ID", axis=1)

    for key, value in metadata["reference"][0].items():
        df[f"ref{key.capitalize()}"] = value

    return df

def combine(inputDir: Path, outputFilePath: Path):
    writer = BigFileWriter(outputFilePath)
    writer.populateFromFolder()
    completedSubfiles = writer.getSubfileNames()
    progress = SteppableProgressBar(sum(1 for _ in inputDir.iterdir()))

    for file in inputDir.iterdir():
        trimmedName = file.name.rsplit(".", 2)[0].rsplit("_", 2)[0]

        if trimmedName not in completedSubfiles:
            extractedFile = zip.extract(file, outputFilePath.parent, verbose=False)
            vcfDF = parseVCF(extractedFile)
            writer.writeDF(vcfDF, trimmedName)
            extractedFile.unlink()

        progress.update()
    
    writer.oneFile()
