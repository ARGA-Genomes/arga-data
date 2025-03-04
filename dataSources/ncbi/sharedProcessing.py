from pathlib import Path
from lib.tools.logger import Logger
from lib.processing.stages import File
from lib.tools.bigFileWriter import BigFileWriter
import time
import pandas as pd
from lib.tools.progressBar import ProgressBar
from threading import Thread
from queue import Queue
from typing import Generator
import requests
import numpy as np
from lib.processing.mapping import Event

def enrichStats(summaryFile: File, outputPath: Path, apiKeyPath: Path = None):
    if apiKeyPath is not None and apiKeyPath.exists():
        Logger.info("Found API key")
        with open(apiKeyPath) as fp:
            apiKey = fp.read().rstrip("\n")
        maxRequests = 10
    else:
        Logger.info("No API key found, limiting requests to 3/second")
        apiKey = ""
        maxRequests = 3

    accessionCol = "#assembly_accession"
    df = summaryFile.loadDataFrame(dtype=object)

    subFile = outputPath.parent / "apiData.csv"
    writer = BigFileWriter(subFile)
    writer.populateFromFolder(writer.subfileDir)
    writtenFileCount = len(writer.writtenFiles)

    summaryFields = {
        "assembly_name": "asm_name",
        "pa_accession": "gbrs_paired_asm",
        "total_number_of_chromosomes": "replicon_count",
        "number_of_scaffolds": "scaffold_count",
        "number_of_component_sequences": "contig_count",
        "provider": "annotation_provider",
        "name": "annotation_name",
        "assembly_type": "assembly_type",
        "gc_percent": "gc_percent",
        "total_gene_count": "total_gene_count",
        "protein_coding_gene_count": "protein_coding_gene_count",
        "non_coding_gene_count": "non_coding_gene_count"
    }

    progress = ProgressBar()
    totalRecords = len(df)

    recordsPerSubsection = 30000
    iterations = (totalRecords / recordsPerSubsection).__ceil__()

    queue = Queue()
    for iteration in range(writtenFileCount, iterations):
        workerRecordCount = (recordsPerSubsection / maxRequests).__ceil__()
        workers: dict[int, Thread] = {}

        for i in range(maxRequests):
            startRange = (i + iteration) * workerRecordCount
            endRange = (i + iteration + 1) * workerRecordCount
            accessions = (accession for accession in df[accessionCol][startRange:endRange])
            worker = Thread(target=apiWorker, args=(i, accessions, apiKey, set(summaryFields), queue), daemon=True)
            worker.start()
            workers[i] = worker
            time.sleep(1 / maxRequests)

        recordData = []
        failedAccessions = []
        while workers:
            value = queue.get()
            if isinstance(value, tuple): # Worker done idx and failed entries
                idx, failed = value
                worker = workers.pop(idx)
                worker.join()
                failedAccessions.extend(failed)
            else: # Record
                recordData.append(value)
                
            progress.update((len(recordData) + len(failedAccessions)) / recordsPerSubsection)

        print(f"\nCompleted subsection {iteration}")
        writer.writeDF(pd.DataFrame.from_records(recordData))

    writer.oneFile(True)
    # print(f"Failed: {failedAccessions}")

    df.merge(pd.read_csv(subFile), how="outer", left_on="#assembly_accession", right_on="current_accession").to_csv(outputPath, index=False)

def apiWorker(idx: int, accessions: Generator[str, None, None], apiKey: str, dropKeys: set, queue: Queue) -> list[dict]:
    session = requests.Session()
    failed = []
    for accession in accessions:
        url = f"https://api.ncbi.nlm.nih.gov/datasets/v2alpha/genome/accession/{accession}/dataset_report"
        headers = {
            "accept": "application/json",
            "api-key": apiKey
        }

        try:
            response = session.get(url, headers=headers)
            data = response.json()
            reports = data.get("reports", [])

        except: # Failed to retrieve, skip
            reports = []

        lastRetrieve = time.time()
        if not reports:
            failed.append(accession)
            continue

        record = parseRecord(reports[0])
        record = {key: value for key, value in record.items() if key not in dropKeys} # Drop duplicate keys with summary
        queue.put(record)
        time.sleep(1.05 - (time.time() - lastRetrieve))

    queue.put((idx, failed))

def parseRecord(record: dict) -> dict:
    def _extractKeys(d: dict, keys: list[str], prefix: str = "", suffix: str = "") -> dict:
        retVal = {}
        for key, value in d.items():
            if key not in keys:
                continue

            if prefix and not key.startswith(prefix):
                key = f"{prefix}_{key}"

            if suffix and not key.endswith(suffix):
                key = f"{key}_{suffix}"

            retVal |= {key: value}

        return retVal
    
    def _extractListKeys(l: list[dict], keys: list[str], prefix: str = "", suffix: str = "") -> list:
        retVal = []
        for item in l:
            retVal.append(_extractKeys(item, keys, prefix, suffix))
        return retVal
    
    def _extract(item: any, keys: list[str], prefix: str = "", suffix: str = "") -> any:
        if isinstance(item, list):
            return _extractListKeys(item, keys, prefix, suffix)
        elif isinstance(item, dict):
            return _extractKeys(item, keys, prefix, suffix)
        else:
            raise Exception(f"Unexpected item: {item}")

    # Annotation info
    annotationInfo = record.get("annotation_info", {})
    annotationFields = [
        "busco", # - busco
        "method", # - method
        "name", # - name
        "pipeline", # - pipeline
        "provider", # - provider
        "release_date", # - releaseDate
        "release_version", # - releaseVersion ?
        "software_version", # - softwareVersion
        "stats", # - stats
        "status" # - status
    ]

    annotationSubFields = {
        "busco": [ # - busco
            "busco_lineage", #   - buscoLineage
            "busco_ver", #   - buscoVer
            "complete" #   - complete
        ],
        "stats": { # - stats
            "gene_counts": [ #   - geneCounts
                "non_coding", #   - nonCoding
                "other", #   - other
                "protein_coding", #   - proteinCoding
                "pseudogene", #   - pseudogene
                "total" #   - total
            ]
        }
    }

    annotationInfo = _extract(annotationInfo, annotationFields)
    annotationInfo |= _extract(annotationInfo.pop("busco", {}), annotationSubFields["busco"], "busco")
    annotationInfo |= _extract(annotationInfo.pop("stats", {}).get("gene_counts", {}), annotationSubFields["stats"]["gene_counts"], suffix="gene_count")

    # Assembly info
    assemblyInfo = record.get("assembly_info", {})
    assemblyFields = [
        "assembly_name", # - assemblyName
        "assembly_status", # - assemblyStatus
        "assembly_type", # - assemblyType
        "description", # - description ?
        "synonym", # - synonym ?
        "paired_assembly", # - pairedAssembly
        "linked_assemblies", # - linkedAssemblies repeated ?
        "diploid_role", # - diploidRole ?
        "atypical", # - atypical ?
        "genome_notes", # - genomeNotes repeated
        "sequencing_tech", # - sequencingTech
        "assembly_method", # - assemblyMethod
        "comments", # - comments
        "suppression_reason" # - suppressionReason ?
    ]

    assemblySubFields = {
        "paired_assembly": [ # - pairedAssembly
            "accession", #   - accession
            "only_genbank", #   - onlyGenbank
            "only_refseq", #   - onlyRefseq ?
            "changed", #   - Changed ?
            "manual_diff", #   - manualDiff ?
            "status", #   - status
        ],
        "linked_assemblies": [ # - linkedAssemblies repeated ?
            "linked_assembly", #   - linkedAssembly ?
            "assembly_type" #   - assemblyType ?
        ],
        "atypical": [ # - atypical ?
            "is_atypical", #   - isAtypical ?
            "warnings" #   - warnings repeated ?
        ]
    }

    assemblyInfo = _extract(assemblyInfo, assemblyFields)
    assemblyInfo |= _extract(assemblyInfo.pop("paired_assembly", {}), assemblySubFields["paired_assembly"], "pa")
    assemblyInfo |= _extract(assemblyInfo.pop("linked_assemblies", {}), assemblySubFields["linked_assemblies"], "la")
    assemblyInfo |= _extract(assemblyInfo.pop("atypical", {}), assemblySubFields["atypical"], "at")

    assemblyStats = record.get("assembly_stats", {}) # Unpack normally

    currentAccession = {"current_accession": record.get("current_accession", "")} # Should always exist

    # May not exist
    organelleInfo = record.get("organelle_info", []) # - organelleInfo ?
    organelleInfoFields = [
        "description", #   - description ?
        "submitter", #    - submitter ?
        "total_seq_length", #    - totalSeqLength ?
        "bioproject" #    - Bioproject related
    ]

    organelleData = {}
    for info in _extract(organelleInfo, organelleInfoFields, "organelle"):
        organelleData[info.pop("description", "Unknown")] = info

    typeMaterial = record.get("type_material", {}) # - typeMaterial ?
    typeMaterialFields = [
        "type_label", #   - typeLabel
        "type_display_text", #   - typeDisplayText
    ]

    typeMaterial = _extract(typeMaterial, typeMaterialFields)

    return annotationInfo | assemblyInfo | assemblyStats | currentAccession | organelleData | typeMaterial

def genbankAugment(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace("na", np.NaN)
    
    fillNA = {
        Event.ASSEMBLIES: "sequence_id",
        Event.ANNOTATION: "sequence_id",
        Event.DEPOSITION: "sequence_id",
        Event.SEQUENCE: "record_id"
    }

    for event, column in fillNA.items():
        if column not in df[event]:
            df[(event, column)] = np.NaN
            
        df[(event, column)].fillna(df[(Event.ASSEMBLIES, "dataset_id")], inplace=True)

    df[(Event.SEQUENCE, "dna_extract_id")] = df[(Event.DEPOSITION, "dataset_id")]
    df = df.drop((Event.DEPOSITION, "dataset_id"), axis=1)

    df[(Event.SEQUENCE, "scientific_name")] = df[(Event.COLLECTION, "scientific_name")]

    return df
