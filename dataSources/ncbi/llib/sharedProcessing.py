import sys
import logging
import requests
import pandas as pd
from pathlib import Path
from requests.adapters import HTTPAdapter, Retry
from multiprocessing import Process, Queue
from lib.secrets import secrets
from lib.bigFiles import RecordWriter
from lib.progressBar import ProgressBar
from lib.processing.files import DataFile
from llib.apiWorker import apiWorker

def getStats(summaryFile: DataFile, outputPath: Path):
    apiKey = secrets.ncbi.key
    if not isinstance(apiKey, str):
        logging.error("No API key found in secrets file, and is required to access NCBI api. Please update 'secrets.toml' with 'key' field under 'ncbi'.")
        return
    
    logging.info("Found API key")
    processes = 10
    recordsPerCall = 200
    recordsPerSubsection = 30000
    accessionCol = "#assembly_accession"

    logging.info("Reading summary file")
    df = summaryFile.read(dtype=object, usecols=[accessionCol])
    totalAccessions = df.size

    writer = RecordWriter(outputPath, recordsPerSubsection)
    startingAccession = writer.writtenFileCount() * recordsPerSubsection
    accessionsPerProcess = ((totalAccessions - startingAccession) / processes).__ceil__()

    queue = Queue()
    processList: list[Process] = []
    for processNumber in range(processes):
        start = startingAccession + (processNumber * accessionsPerProcess)
        end = start + accessionsPerProcess
        accessions = df[accessionCol].iloc[start:end].tolist()
        p = Process(target=apiWorker, args=(queue, processNumber, apiKey, recordsPerCall, accessions), daemon=True)
        p.start()
        processList.append(p)
    logging.info(f"Started {len(processList)} workers")

    progress = ProgressBar(totalAccessions - startingAccession)
    try:
        while processes > 0:
            data = queue.get()
            if isinstance(data, int): # Termination ID
                processList[data].join()
                processes -= 1
                continue

            writer.write(data)
            progress.update()

    except KeyboardInterrupt:
        for process in processList:
            process.join()
        print()
        logging.info("Cleaned up workers")

        return

    writer.combine(removeParts=False, index=False)

def merge(summaryFile: DataFile, statsFilePath: Path, outputPath: Path) -> None:
    if not summaryFile.exists():
        logging.error("Unable to merge files as summary file doesn't exist")
        return
    
    if not statsFilePath.exists():
        logging.error("Unable to merge files as stats file doesn't exist")
        return

    df = summaryFile.read(low_memory=False)
    df2 = pd.read_csv(statsFilePath, low_memory=False)
    df = df.merge(df2, how="outer", left_on="#assembly_accession", right_on="current_accession")
    df.to_csv(outputPath, index=False)

def cleanData(inPath: Path, outPath: Path) -> None:
    df = pd.read_csv(inPath)
    df = df.replace("na", pd.NaN)
    
    for column in ("sequence_id", "record_id"):
        if column not in df.columns:
            df[column] = pd.NaN

        df = df[column].fillna(df["dataset_id"])

    df = df.rename({"dataset_id": "dna_extract_id"}, axis=1)
    df.to_csv(outPath, index=False)
