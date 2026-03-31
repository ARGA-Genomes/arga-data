from pathlib import Path
import lib.zipping as zip
import pandas as pd
from lib.progressBar import ProgressBar
import mmap
from lib.bigFiles import DFWriter, combineDirectoryFiles

def _parseVCF(filePath: Path, outputDir: Path) -> None:
    trimmedName = filePath.name[:len("_curent_ids.vcf.gz")] # Trim section after accession
    outputFile = f"{trimmedName}.parquet"

    writer = DFWriter(outputDir / outputFile, subDirName=f"{trimmedName}_chunks")
    if writer.outputFile.exists():
        return

    extractedFile = zip.extract(filePath, outputDir, verbose=False)

    contigs = {}
    reference = {}
    with open(extractedFile, "rb") as fp:
        with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mfp:
            tableStart = mfp.find(b"#CHROM")
            header = mfp.read(tableStart).decode()
            rows = header.count("\n")

            for line in header.split("\n"):
                if line.startswith("##contig="):
                    key, value = line[len("##contig=<ID="):-len("\">")].replace("accession=\"", "").split(",")
                    contigs[key] = value

                elif line.startswith("##reference="):
                    for item in line[len("##reference=<"):-len(">")].split(","):
                        key, value = item.split("=", 1)
                        reference[f"REFERENCE_{key.upper()}"] = value

    for df in pd.read_csv(filePath, header=rows, sep="\t", chunksize=500000, na_values=["."], low_memory=False):
        df: pd.DataFrame = df.fillna("")

        df["CONTIG"] = df["#CHROM"].map(contigs)
        for key, value in reference.items():
            df[key] = value

        writer.write(df)

    writer.combine(removeParts=True, verbose=False)
    extractedFile.unlink()

def combine(inputDir: Path, outputFilePath: Path):
    progress = ProgressBar(sum(1 for _ in inputDir.iterdir()))

    for file in inputDir.iterdir():
        _parseVCF(file, outputFilePath.parent)
        progress.update()

    combineDirectoryFiles(outputFilePath, outputFilePath.parent, "*.parquet")
