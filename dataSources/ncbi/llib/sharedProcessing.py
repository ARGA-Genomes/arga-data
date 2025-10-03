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
    processes = 10
    if apiKey is None:
        apiKey = "" # Use empty string
        processes = 3 # Reduce processes count due to reduced api call rate
        logging.info("No API key found")

    else:
        logging.info("Found API key")

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

    writer.combine(False, index=False)

def merge(summaryFile: DataFile, statsFilePath: Path, outputPath: Path) -> None:
    df = summaryFile.read(low_memory=False)
    df2 = pd.read_csv(statsFilePath, low_memory=False)
    df = df.merge(df2, how="outer", left_on="#assembly_accession", right_on="current_accession")
    df.to_csv(outputPath, index=False)

def genbankAugment(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace("na", pd.NaN)
    
    fillNA = {
        "assembly": "sequence_id",
        "annotation": "sequence_id",
        "record level": "sequence_id",
        "sequencing": "record_id"
    }

    for event, column in fillNA.items():
        if column not in df[event]:
            df[(event, column)] = pd.NaN
            
        df[(event, column)].fillna(df[("assembly", "dataset_id")], inplace=True)

    df[("sequencing", "dna_extract_id")] = df[("record level", "dataset_id")]
    df = df.drop(("record level", "dataset_id"), axis=1)

    df[("sequencing", "scientific_name")] = df[("collection", "scientific_name")]

    return df
