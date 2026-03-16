import sys
import logging
import requests
from multiprocessing import Queue
from requests.adapters import HTTPAdapter, Retry

def apiWorker(queue: Queue, id: int, apiKey: str, recordsPerCall: int, accessions: list[str]):
    headers = {
        "accept": "application/json",
        "api-key": apiKey
    }

    params = {
        "page_size": recordsPerCall
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

    # Suppress logs about retrying urls
    logging.getLogger("requests").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1)
    session.mount("https://", HTTPAdapter(max_retries=retries))

    collectionAmount = (len(accessions) / recordsPerCall).__ceil__()
    accessionStrings = []
    for collectionNumber in range(collectionAmount):
        accessionStrings.append("%2C".join(accessions[collectionNumber*recordsPerCall:(collectionNumber+1)*recordsPerCall]))

    try:
        for string in accessionStrings:
            url = f"https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession/{string}/dataset_report"
            response = session.get(url, headers=headers, params=params)
            data = response.json()
            records = data.get("reports", [])
            for record in records:
                queue.put(parseRecord(record, list(summaryFields)))
    except KeyboardInterrupt:
        pass

    queue.put(id)

def parseRecord(record: dict, excludeFields: list) -> dict:
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

    annotationInfo: dict = _extract(annotationInfo, annotationFields)
    annotationInfo.update(_extract(annotationInfo.pop("busco", {}), annotationSubFields["busco"], "busco"))
    annotationInfo.update(_extract(annotationInfo.pop("stats", {}).get("gene_counts", {}), annotationSubFields["stats"]["gene_counts"], suffix="gene_count"))

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

    assemblyInfo.update(_extract(assemblyInfo.pop("paired_assembly", {}), assemblySubFields["paired_assembly"], "pa"))
    assemblyInfo.update(_extract(assemblyInfo.pop("linked_assemblies", {}), assemblySubFields["linked_assemblies"], "la"))
    assemblyInfo.update(_extract(assemblyInfo.pop("atypical", {}), assemblySubFields["atypical"], "at"))

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
    recordData = annotationInfo | assemblyInfo | assemblyStats | currentAccession | organelleData | typeMaterial
    return {key: value for key, value in recordData.items() if key not in excludeFields}
