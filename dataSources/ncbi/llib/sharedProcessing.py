from pathlib import Path
import logging
from lib.processing.files import DataFile
from lib.bigFiles import RecordWriter
import time
import pandas as pd
from lib.progressBar import ProgressBar
import requests
from requests.adapters import HTTPAdapter, Retry
import numpy as np
from lib.secrets import secrets
import concurrent.futures as cf

def getStats(summaryFile: DataFile, outputPath: Path):
    if secrets.ncbi.key is None:
        apiKey = ""
        maxWorkers = 3
        logging.info("No API key found, limiting requests to 3/second")
    else:
        apiKey = secrets.ncbi.key
        maxWorkers = 10
        logging.info("Found API key")

    recordsPerCall = 200
    recordsPerSubsection = 30000
    accessionCol = "#assembly_accession"

    df = summaryFile.read(dtype=object, usecols=[accessionCol])
    totalAccessions = df.size

    writer = RecordWriter(outputPath, recordsPerSubsection)
    writtenFileCount = writer.writtenFileCount()

    startingAccession = writtenFileCount * recordsPerSubsection
    totalCalls = ((totalAccessions - startingAccession) / recordsPerCall).__ceil__()

    def generateURLs() -> str:
        for call in range(totalCalls):
            start = startingAccession + (call * recordsPerCall)
            end = startingAccession + ((call + 1) * recordsPerCall)
            passAccessions = '%2C'.join(df[accessionCol].to_list()[start:end])
            yield f"https://api.ncbi.nlm.nih.gov/datasets/v2alpha/genome/accession/{passAccessions}/dataset_report?page_size={recordsPerCall}"

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1)
    session.mount("https://", HTTPAdapter(max_retries=retries))

    headers = {
        "accept": "application/json",
        "api-key": apiKey
    }

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

    def getRecords(url: str) -> list[dict]:
        response = session.get(url, headers=headers)
        data = response.json()
        reports = data.get("reports", [])
        records = []
        for record in reports:
            record = parseRecord(record)
            record = {key: value for key, value in record.items() if key not in summaryFields} # Drop duplicate keys with summary
            records.append(record)

        return records

    # Suppress logs about retrying urls
    logging.getLogger("requests").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)

    progress = ProgressBar(df.size - (writtenFileCount * recordsPerSubsection))
    with cf.ThreadPoolExecutor(max_workers=maxWorkers) as executor:
        futures = (executor.submit(getRecords, url) for url in generateURLs())
        for future in cf.as_completed(futures):
            records = future.result()
            writer.writerMultipleRecords(records)
            progress.update(len(records))
            time.sleep(1 / maxWorkers)

    writer.combine(True, index=False)

def merge(summaryFile: DataFile, statsFilePath: Path, outputPath: Path) -> None:
    df = summaryFile.read(low_memory=False)
    df2 = pd.read_csv(statsFilePath, low_memory=False)
    df = df.merge(df2, how="outer", left_on="#assembly_accession", right_on="current_accession")
    df.to_csv(outputPath, index=False)

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

    assemblyInfo: dict = _extract(assemblyInfo, assemblyFields)
    assemblyInfo["comments"] = assemblyInfo.get("comments", "").replace("\n", "").replace("\t", "")

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
        "assembly": "sequence_id",
        "annotation": "sequence_id",
        "record level": "sequence_id",
        "sequencing": "record_id"
    }

    for event, column in fillNA.items():
        if column not in df[event]:
            df[(event, column)] = np.NaN
            
        df[(event, column)].fillna(df[("assembly", "dataset_id")], inplace=True)

    df[("sequencing", "dna_extract_id")] = df[("record level", "dataset_id")]
    df = df.drop(("record level", "dataset_id"), axis=1)

    df[("sequencing", "scientific_name")] = df[("collection", "scientific_name")]

    return df
