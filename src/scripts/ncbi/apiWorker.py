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
                queue.put(parseRecord(record))
    except KeyboardInterrupt:
        pass

    queue.put(id)

def parseRecord(record: dict) -> dict:
    recordData = {"accession": record["accession"]}

    # Annotation info
    annotationInfo = record.get("annotation_info", {})
    recordData["annotation_method"] = annotationInfo.get("method", "")
    recordData["annotation_pipeline"] = annotationInfo.get("pipeline", "")
    recordData["annotation_software_version"] = annotationInfo.get("software_version", "")
    recordData["annotation_status"] = annotationInfo.get("status")
    recordData["annotation_report_url"] = annotationInfo.get("report_url", "")

    # Annotation Gene Counts
    geneCounts: dict = annotationInfo.get("stats", {}).get("gene_counts", {})
    recordData["gc_pseudogene"] = geneCounts.get("pseudogene", "")
    recordData["gc_other"] = geneCounts.get("other", "")

    # Annotation Busco
    buscoInfo: dict = annotationInfo.get("busco", {})
    recordData["busco_lineage"] = buscoInfo.get("busco_lineage", "")
    recordData["busco_ver"] = buscoInfo.get("busco_ver", "")
    recordData["busco_complete"] = buscoInfo.get("complete", "")
    recordData["busco_single_copy"] = buscoInfo.get("single_copy", "")
    recordData["busco_duplicated"] = buscoInfo.get("duplicated", "")
    recordData["busco_fragmented"] = buscoInfo.get("fragmented", "")
    recordData["busco_missing"] = buscoInfo.get("missing", "")
    recordData["busco_total_count"] = buscoInfo.get("total_count", "")

    # Assembly Info
    assemblyInfo: dict = record.get("assembly_info", {})
    recordData["asm_status"] = assemblyInfo.get("assembly_status", "")
    recordData["bioproject_accession"] = assemblyInfo.get("bioproject_accession", "")
    recordData["sequencing_tech"] = assemblyInfo.get("sequencing_tech", "")
    recordData["assembly_method"] = assemblyInfo.get("assembly_method", "")
    
    pairedAssembly: dict = assemblyInfo.get("paired_assembly", {})
    recordData["paired_asm_status"] = pairedAssembly.get("status", "")

    biosample: dict = assemblyInfo.get("biosample", {})
    recordData["biosample_accession"] = biosample.get("accession", "")

    # Assembly Stats
    assemblyStats: dict = record.get("assembly_stats", {})
    recordData["contig_n50"] = assemblyStats.get("contig_n50", "")
    recordData["contig_l50"] = assemblyStats.get("contig_l50", "")
    recordData["scaffold_n50"] = assemblyStats.get("scaffold_n50", "")
    recordData["scaffold_l50"] = assemblyStats.get("scaffold_l50", "")
    recordData["atgc_count"] = assemblyStats.get("atgc_count", "")
    recordData["gc_count"] = assemblyStats.get("gc_count", "")
    recordData["genome_coverage"] = assemblyStats.get("genome_coverage", "")

    # Organelle Info
    organelleInfo = record.get("organelle_info", [])
    recordData["organelles"] = organelleInfo

    # Type Material
    typeMaterial: dict = record.get("type_material", {})
    recordData["type_material_label"] = typeMaterial.get("type_label", "")
    recordData["type_material_display_text"] = typeMaterial.get("type_display_text", "")

    return recordData
