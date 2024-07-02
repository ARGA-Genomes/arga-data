import requests
import time
import pandas as pd
import concurrent.futures
from pathlib import Path
from io import StringIO
from threading import Thread
from queue import Queue
import lib.commonFuncs as cmn
import lib.dataframeFuncs as dff
import sourceProcessing.ncbiFlatfileParser as ffp
from lib.processing.stages import File
from lib.tools.bigFileWriter import BigFileWriter
from lib.tools.zipping import RepeatExtractor
from lib.tools.progressBar import ProgressBar
from lib.tools.logger import Logger
from typing import Generator
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
    with concurrent.futures.ProcessPoolExecutor(max_workers=20) as executor:
        futures = (executor.submit(parseStats, filePath) for filePath in inputFolder.iterdir())
        for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            print(f"At entry: {idx}", end="\r")

            result = future.result()
            if not result:
                continue

            records.append(result)

            if len(records) >= 100000:
                writer.writeDF(pd.DataFrame.from_records(records))
                records.clear()

    print()
    writer.oneFile()

def parseNucleotide(folderPath: Path, outputFilePath: Path, verbose: bool = True) -> None:
    extractor = RepeatExtractor(outputFilePath.parent)
    writer = BigFileWriter(outputFilePath, "seqChunks", "chunk")

    for idx, file in enumerate(folderPath.iterdir(), start=1):
        if verbose:
            print(f"Extracting file {file.name}")
        else:
            print(f"Processing file: {idx}", end="\r")
    
        extractedFile = extractor.extract(file)

        if extractedFile is None:
            print(f"Failed to extract file {file.name}, skipping")
            continue

        if verbose:
            print(f"Parsing file {extractedFile}")

        df = ffp.parseFlatfile(extractedFile, verbose)
        writer.writeDF(df)
        extractedFile.unlink()

    writer.oneFile()

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

    progress = ProgressBar(50)
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
                
            progress.render((len(recordData) + len(failedAccessions)) / recordsPerSubsection)

        print(f"Completed subsection {iteration}")
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
