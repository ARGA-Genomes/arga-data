import logging
import pandas as pd
from pathlib import Path
from multiprocessing import Process, Queue
from lib.secrets import Secrets
from lib.bigFiles import RecordWriter
from lib.progressBar import ProgressBar
from lib.processing.files import DataFile
from scripts.ncbi.apiWorker import apiWorker
import numpy as np
from lib.processing.scripts import importableScript

@importableScript()
def getStats(outputDir: Path, summaryFile: DataFile):
    secrets = Secrets("ncbi")

    if not secrets.key:
        logging.error("No API key found in secrets file, and is required to access NCBI api. Please update 'secrets.toml' with 'key' field under 'ncbi'.")
        return
    
    logging.info("Found API key")
    processes = 10
    recordsPerCall = 200
    recordsPerSubsection = 30000
    accessionCol = "#assembly_accession"

    logging.info("Reading summary file")
    df = summaryFile.read(dtype=object, usecols=[accessionCol], header=1)
    totalAccessions = df.size

    writer = RecordWriter(outputDir / summaryFile.path.name, recordsPerSubsection)
    startingAccession = writer.writtenFileCount() * recordsPerSubsection
    accessionsPerProcess = ((totalAccessions - startingAccession) / processes).__ceil__()

    queue = Queue()
    processList: list[Process] = []
    for processNumber in range(processes):
        start = startingAccession + (processNumber * accessionsPerProcess)
        end = start + accessionsPerProcess
        accessions = df[accessionCol].iloc[start:end].tolist()
        p = Process(target=apiWorker, args=(queue, processNumber, secrets.key, recordsPerCall, accessions), daemon=True)
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

@importableScript(inputCount=2)
def merge(outputDir: Path, summaryFile: DataFile, statsFile: DataFile, fileName: str) -> None:
    if not summaryFile.exists():
        logging.error("Unable to merge files as summary file doesn't exist")
        return
    
    if not statsFile.exists():
        logging.error("Unable to merge files as stats file doesn't exist")
        return

    df = summaryFile.read(header=1, low_memory=False)
    df2 = statsFile.read(low_memory=False)

    df = df.merge(df2, how="outer", left_on="#assembly_accession", right_on="current_accession")
    df.to_csv(outputDir / fileName, index=False)

@importableScript()
def cleanData(outputDir: Path, mergedData: DataFile, fileName: str) -> None:
    df = mergedData.read(low_memory=False)
    df = df.replace("na", np.NaN)
    
    for column in ("sequence_id", "record_id"):
        df[column] = np.NaN
        df[column] = df[column].fillna(df["#assembly_accession"])

    df = df.rename({"biosample": "dna_extract_id"}, axis=1)
    df.to_csv(outputDir / fileName, index=False)
